// group.cpp

/**
*    Copyright (C) 2012 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#include "mongo/platform/basic.h"

#include <zmq.hpp>
#include <boost/thread.hpp>
#include <thread>

#include "mongo/base/string_data.h"
#include "mongo/db/commands.h"
#include "mongo/db/server_options.h"
#include "mongo/util/concurrency/msg.h"
#include "mongo/util/scopeguard.h"
#include "mongo/base/init.h"
#include "mongo/s/config.h"
#include "mongo/s/mongos_options.h"
#include "mongo/db/commands/zmq.h"

namespace mongo {
    zmq::context_t zmq_context (1);

    const auto PUB_ENDPOINT = "inproc://pub";
    const auto SUB_ENDPOINT = "inproc://sub";

    void zmq_publish(const StringData prefix, BSONObj payload) {
        auto wrapped = BSON("msg" << payload);
        auto body = wrapped.firstElement();
        auto sock = zmq::socket_t(zmq_context, ZMQ_PUSH);
        sock.connect(PUB_ENDPOINT);
        invariant(sock.send(prefix.rawData(), prefix.size(), ZMQ_SNDMORE));
        invariant(sock.send(body.rawdata(), body.size())); // just the element
    }

    void proxy(zmq::socket_t back, zmq::socket_t front) {
        zmq::proxy(back, front, NULL);
    }

    void zmq_shard_proxy_thread() {
        auto config_servers = configServer.getConnectionString().getServers();
        if(config_servers.size() > 1) {
            config_servers.erase(config_servers.begin());
        }

        zmq::socket_t int_sub(zmq_context, ZMQ_SUB);
        zmq::socket_t int_push(zmq_context, ZMQ_PUSH);

        int_sub.setsockopt(ZMQ_SUBSCRIBE, "", 0);
        int_sub.connect(SUB_ENDPOINT);
        
        for(const auto &server: config_servers) {
            auto config_host_port = HostAndPort(server);
            auto config_push_endpoint = HostAndPort(config_host_port.host(), config_host_port.port() + 2000);

            int_push.connect(("tcp://" + config_push_endpoint.toString()).c_str());
        }

        std::thread(proxy, std::move(int_sub), std::move(int_push)).detach();
    }

namespace {
    typedef long long CursorId;

    auto nextCursor = CursorId(1);
    auto cursorMap = std::map<CursorId, zmq::socket_t>();

    CursorId registerSock(zmq::socket_t sock) {
        const auto id = nextCursor++;
        invariant(cursorMap.count(id) == 0);
        cursorMap.insert(std::make_pair(id, std::move(sock)));
        return id;
    }

    class SocketCheckout {
    public:
        SocketCheckout(CursorId id, zmq::socket_t sock)
            :id(id)
            ,sock(std::move(sock))
        { }
        
        SocketCheckout(SocketCheckout&& other) 
            :id(other.id)
            ,sock(std::move(other.sock))
        { }

        void returnToMap() {
            invariant(sock);
            cursorMap.find(id)->second = std::move(sock);
        }

        ~SocketCheckout() {
            if (sock)
                cursorMap.erase(id);
        }

        const CursorId id;
        zmq::socket_t sock;
    };

    SocketCheckout checkOut(CursorId id) {
            auto sockIt = cursorMap.find(id);
            uassert(21002, "no such cursor",
                    sockIt != cursorMap.end());
            uassert(21004, "cursor busy",
                    sockIt->second);
            
            return SocketCheckout(id, std::move(sockIt->second));
    }

}

    class PubCommand : public Command {
    public:
        PubCommand() : Command("pub") {}
        virtual LockType locktype() const { return NONE; }
        virtual bool slaveOk() const { return false; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual void help( stringstream &help ) const {
            help << "";
        }
        Status checkAuthForCommand(ClientBasic* client,
                                   const std::string& dbname,
                                   const BSONObj& cmdObj) {
            return Status::OK();
        }

        bool run(const string& dbname, BSONObj& request, int, string& errmsg, BSONObjBuilder& result, bool) {
            const auto prefix = request.firstElement().String();
            const auto body = request["msg"];
            uassert(21000, "You must supply a 'msg' field",
                    !body.eoo());

            auto sock = zmq::socket_t(zmq_context, ZMQ_PUSH);
            sock.connect(PUB_ENDPOINT);
            invariant(sock.send(prefix.data(), prefix.size(), ZMQ_SNDMORE));
            invariant(sock.send(body.rawdata(), body.size())); // just the element
            return true;
        }

    } cmdPub;

    class SubCommand : public Command {
    public:
        SubCommand() : Command("sub") {}
        virtual LockType locktype() const { return NONE; }
        virtual bool slaveOk() const { return false; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual void help( stringstream &help ) const {
            help << "";
        }
        Status checkAuthForCommand(ClientBasic* client,
                                   const std::string& dbname,
                                   const BSONObj& cmdObj) {
            return Status::OK();
        }

        bool run(const string& dbname, BSONObj& request, int, string& errmsg, BSONObjBuilder& result, bool) {
            const auto subscriptions = request.firstElement();
            auto cursorId = request.hasField("id") ? request["id"].Long() : 0LL;
            if (cursorId) {
                auto checkout = checkOut(cursorId);
                addSub(checkout.sock, subscriptions);
                checkout.returnToMap();
            } else {
                auto sock = zmq::socket_t(zmq_context, ZMQ_SUB);
                addSub(sock, subscriptions);
                sock.connect(SUB_ENDPOINT);
                cursorId = registerSock(std::move(sock));
            }

            result.append("cursorId", cursorId);

            return true;
        }
        
        void addSub(zmq::socket_t& sock, const BSONElement& elem) {
            if (elem.type() == String) {
                const auto prefix = elem.String();
                sock.setsockopt(ZMQ_SUBSCRIBE, prefix.data(), prefix.size());
            } else if (elem.type() == Array) {
                BSONForEach(sub, elem.Obj()) {
                    const auto prefix = sub.String();
                    sock.setsockopt(ZMQ_SUBSCRIBE, prefix.data(), prefix.size());
                }
            } else {
                uasserted(21005, string("invalid subscription type: ") + typeName(elem.type()));
            }
        }

    } cmdSub;

    class UnsubCommand : public Command {
    public:
        UnsubCommand() : Command("unsub") {}
        virtual LockType locktype() const { return NONE; }
        virtual bool slaveOk() const { return false; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual void help( stringstream &help ) const {
            help << "";
        }
        Status checkAuthForCommand(ClientBasic* client,
                                   const std::string& dbname,
                                   const BSONObj& cmdObj) {
            return Status::OK();
        }

        bool run(const string& dbname, BSONObj& request, int, string& errmsg, BSONObjBuilder& result, bool) {
            auto id = request.firstElement().Long();
            auto sock = checkOut(id);
            // not calling sock.returnToMap() kills it
            return true;
        }

    } cmdUnsub;

    class PollCommand : public Command {
    public:
        PollCommand() : Command("poll") {}
        virtual LockType locktype() const { return NONE; }
        virtual bool slaveOk() const { return false; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual void help( stringstream &help ) const {
            help << "";
        }
        Status checkAuthForCommand(ClientBasic* client,
                                   const std::string& dbname,
                                   const BSONObj& cmdObj) {
            return Status::OK();
        }

        bool run(const string& dbname, BSONObj& request, int, string& errmsg, BSONObjBuilder& result, bool) {
            auto id = request.firstElement().Long();
            auto timeout = request.hasField("timeout") ? request["timeout"].safeNumberLong() : -1LL;

            auto sock = checkOut(id);

            //  Initialize poll set
            zmq::pollitem_t items [] = {
                { sock.sock, 0, ZMQ_POLLIN, 0 }
            };
            zmq::poll(&items [0], 1, timeout);

            //  Process messages from both sockets
            auto msg = zmq::message_t();
            BSONArrayBuilder messages;
            while (sock.sock.recv(&msg, ZMQ_DONTWAIT)) {
                BSONObjBuilder message(messages.subobjStart());

                // get the prefix
                const auto msgName = StringData(static_cast<const char*>(msg.data()), msg.size());
                invariant(msg.more());
                message.append("name", msgName);
                msg.rebuild();

                // get the body
                invariant(sock.sock.recv(&msg));
                invariant(!msg.more());
                const auto body = BSONElement(static_cast<const char*>(msg.data()));
                invariant(size_t(body.size()) == msg.size());
                invariant(body.fieldNameStringData() == "msg");
                message.append(body);
                msg.rebuild();
            }
            
            result.append("messages", messages.arr());
            sock.returnToMap();

            return true;
        }

    } cmdPoll;

} // namespace mongo
