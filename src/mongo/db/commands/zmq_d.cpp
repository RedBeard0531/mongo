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
#include <thread>

#include "mongo/base/string_data.h"
#include "mongo/db/commands.h"
#include "mongo/db/server_options.h"
#include "mongo/util/concurrency/msg.h"
#include "mongo/util/scopeguard.h"
#include "mongo/base/init.h"
#include "mongo/db/commands/zmq.h"

namespace mongo {
namespace {
    void proxy(zmq::socket_t back, zmq::socket_t front) {
        zmq::proxy(back, front, NULL);
    }

    MONGO_INITIALIZER(ZMQBGThread)(::mongo::InitializerContext* context) {
        const auto port = serverGlobalParams.port;
        const std::string PUB_EXT_ENDPOINT = str::stream() << "tcp://*:" << (port + 2000);
        const std::string SUB_EXT_ENDPOINT = str::stream() << "tcp://*:" << (port + 3000);

        zmq::socket_t int_pull(zmq_context, ZMQ_PULL);
        int_pull.bind(PUB_ENDPOINT);
        int_pull.bind(PUB_EXT_ENDPOINT.c_str());

        zmq::socket_t ext_pub(zmq_context, ZMQ_PUB);
        ext_pub.bind(SUB_ENDPOINT);
        ext_pub.bind(SUB_EXT_ENDPOINT.c_str());

        std::thread(proxy, std::move(int_pull), std::move(ext_pub)).detach();
        return Status::OK();
    }
}
}
