/*    Copyright 2014 MongoDB Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#pragma once

#include <exception>
#include <memory>
#include <boost/optional.hpp>
#include <system_error>
#include <third_party/lmdb/lmdb.h>

#include "mongo/bson/inline_decls.h"
#include "mongo/util/stacktrace.h"

namespace mongo {
namespace mdb {
    using Stats = MDB_stat;

    inline static std::error_category& get_error_category() {
        class error_category : public std::error_category {
        public:
            virtual const char* name() const noexcept { return "MDB"; }
            virtual std::string message(int code) const noexcept { return mdb_strerror(code); }
        };
        static error_category instance;
        return instance;
    }


    /// Specialize me!
    template <typename T>
    struct Adapter;
        // T fromMDB(const Data&) = delete; // must be const&
        // Data toMDB(const T&) = delete;

    /**
     * Define either or both of these two functions to support your type directly in these functions
     */
    struct Data : MDB_val {
        Data() : MDB_val{0, nullptr} {}
        Data(size_t size, const void* ptr) : MDB_val{size, const_cast<void*>(ptr)} {}
        /*implicit*/ Data(const MDB_val& base) : MDB_val(base) {}

        template <typename T>
        /*implicit*/ Data(const T& from) : MDB_val( Adapter<T>().toMDB(from)) {} 
        template <typename T>
        T as() const { return Adapter<T>().fromMDB(*this); }
    };

    using KV = std::pair<Data, Data>;
    using MaybeKV = boost::optional<KV>;

    class Error : public std::system_error {
    public:
        explicit Error(int rc) : std::system_error(rc, get_error_category()) {}
    };

    __attribute__((noinline)) inline void fail(int code) {
        std::cout << "MDB ERROR: " << code << " " << mdb_strerror(code) << std::endl;
        printStackTrace();
        throw Error(code);
    }
    inline void check(int ret) { if (MONGO_unlikely(ret != MDB_SUCCESS)) fail(ret);}

    template <typename T, void(*func)(T*)>
    struct funcType {
        void operator () (T* obj) { func(obj); }
    };

    template <typename T, void(*func)(T*)>
    using unique_ptr_with_deleter = std::unique_ptr<T, funcType<T, func>>;

    class Env {
    public:
        Env() { 
            MDB_env* temp;
            check(mdb_env_create(&temp));
            _env.reset(temp);
        }

        void open(const char* path, unsigned int flags, mdb_mode_t mode=0660) {
            check(mdb_env_open(get(), path, flags, mode));
        }
        
        // TODO mdb_env_copy
        // TODO mdb_env_copy_fd

        Stats stats() const {
            Stats stat;
            check(mdb_env_stat(get(), &stat));
            return stat;
        }

        MDB_envinfo info() const {
            MDB_envinfo info;
            check(mdb_env_info(get(), &info));
            return info;
        }

        // TODO mdb_env_info

        void sync(bool force=true) { check(mdb_env_sync(get(), force)); }

        // TODO mdb_env_set_flags
        // TODO mdb_env_get_flags
        // TODO mdb_env_get_path
        // TODO mdb_env_get_fd
        // TODO mdb_env_set_mapsize (maybe)
        // TODO mdb_env_set_maxreaders (maybe)
        // TODO mdb_env_get_maxreaders
        // TODO mdb_env_set_maxdbs (maybe)
        // TODO mdb_env_get_maxdbs
        // TODO mdb_env_get_maxkeysize

        // TODO mdb_reader_list
        // TODO mdb_reader_check
        
        MDB_env* get() { return _env.get(); }
        /*implicit*/ operator MDB_env* () { return get(); }

    private:
        // be careful with constness
        MDB_env* get() const { return _env.get(); }

        unique_ptr_with_deleter<MDB_env, mdb_env_close> _env;
    };

    class Txn {
    public:
        // Constructors
        Txn() {}
        static Txn Read(MDB_env* env, MDB_txn* parent=nullptr) { return Txn(env, READ, parent); }
        static Txn Write(MDB_env* env, MDB_txn* parent=nullptr) { return Txn(env, WRITE, parent); }

        // TODO mdb_txn_env (maybe)

        MDB_txn* get()  { return _txn.get(); }
        /*implicit*/ operator MDB_txn* () { return get(); }

        /// called by default
        void abort() { _txn.reset(); }
        
        /// must be explicitly called
        void commit() { check(mdb_txn_commit(_txn.release())); }

        void reset() { mdb_txn_reset(get()); }
        void renew() { check(mdb_txn_renew(get())); }

        explicit operator bool const () { return bool(_txn); }

    private:
        enum RW { READ, WRITE };
        Txn(MDB_env* env, RW rw = READ, MDB_txn* parent = nullptr) { 
            MDB_txn* temp;
            check(mdb_txn_begin(env, parent, (rw == READ ? MDB_RDONLY : 0), &temp));
            _txn.reset(temp);
        }

        unique_ptr_with_deleter<MDB_txn, mdb_txn_abort> _txn;
    };

    class DB {
    public:
        DB() {}
        ~DB() {
            if (_db != noDb)
                mdb_dbi_close(_env, _db);
            _db = noDb;
        }

        /// moveable, not copyable
        DB(DB&& other) : _db(other._db), _env(other._env) { other._db = noDb; }
        DB& operator=(DB&& other) {
            this->~DB(); // clean up if needed
            _db = other._db;
            _env = other._env;
            other._db = noDb;
            return *this;
        } 

        // returns true on success
        bool openIfCan(MDB_txn* txn, const char* name, unsigned int flags) {
            this->~DB(); // clean up if needed

            _env = mdb_txn_env(txn);
            int rc = mdb_dbi_open(txn, name, flags, &_db);
            if (rc == MDB_NOTFOUND) return false;
            check(rc);
            return true;
        }
        
        void open(MDB_txn* txn, const char* name, unsigned int flags) {
            this->~DB(); // clean up if needed

            _env = mdb_txn_env(txn);
            check(mdb_dbi_open(txn, name, flags, &_db));
        }

        Stats stats(MDB_txn* txn) const {
            Stats stat;
            check(mdb_stat(txn, get(), &stat));
            return stat;
        }

        // TODO mdb_dbi_flags

        void empty(MDB_txn* txn) { check(mdb_drop(txn, get(), 0/*empty*/)); }
        void drop(MDB_txn* txn) {
            check(mdb_drop(txn, get(), 1/*drop*/));
            _db = noDb;
        }
        
        void setCompare(MDB_txn* txn, MDB_cmp_func* cmp) {
            check(mdb_set_compare(txn, get(), cmp));
        }

        void setDupSort(MDB_txn* txn, MDB_cmp_func* cmp) {
            check(mdb_set_dupsort(txn, get(), cmp));
        }

        void setCompareCtx(MDB_txn* txn, const void* ctx) {
            check(mdb_set_cmpctx(txn, get(), ctx));
        }

        // TODO mdb_dbi_relfunc
        // TODO mdb_dbi_relctx

        bool hasKey(MDB_txn* txn, Data key) {
            int rc = mdb_get(txn, get(), &key, nullptr);
            if (rc == MDB_NOTFOUND) return false;
            check(rc);
            return true;
        }

        Data get(MDB_txn* txn, Data key) const {
            Data ret;
            check(mdb_get(txn, get(), &key, &ret));
            return ret;
        }

        KV put(MDB_txn* txn, Data key, Data value, unsigned int flags=0) {
            auto kv = KV(key, value);
            check(mdb_put(txn, get(), &kv.first, &kv.second, flags));
            return kv;
        }

        void del(MDB_txn* txn, Data key) {
            check(mdb_del(txn, get(), &key, nullptr));
        }
        void del(MDB_txn* txn, Data key, Data data) {
            check(mdb_del(txn, get(), &key, &data));
        }

        // TODO mdb_cmp
        // TODO mdb_dcmp

        MDB_dbi get() const { return _db; }
        /*implicit*/ operator MDB_dbi () const { return get(); }

        explicit operator bool const () { return _db != noDb; }

    private:

        static constexpr auto noDb = MDB_dbi(-1);
        MDB_dbi _db = noDb;
        MDB_env* _env;
    };

    class Cursor {
    public:
        Cursor() {}
        Cursor(MDB_txn* txn, MDB_dbi db) {
            MDB_cursor* temp;
            check(mdb_cursor_open(txn, db, &temp));
            _cursor.reset(temp);
        }

        //
        // Wrappers around mdb_cursor_get
        //

        MaybeKV first() { return simple(MDB_FIRST); }
        MaybeKV firstDup() { return simple(MDB_FIRST_DUP); }

        MaybeKV current() const { return simple(MDB_GET_CURRENT); }
        MaybeKV currentMultiple() const { return simple(MDB_GET_MULTIPLE); }

        MaybeKV last() { return simple(MDB_LAST); }
        MaybeKV lastDup() { return simple(MDB_LAST_DUP); }

        // TODO handle end better (return optional)
        MaybeKV next() { return simple(MDB_NEXT); }
        MaybeKV nextDup() { return simple(MDB_NEXT_DUP); }
        MaybeKV nextMultiple() { return simple(MDB_NEXT_MULTIPLE); }
        MaybeKV nextNoDup() { return simple(MDB_NEXT_NODUP); }
        MaybeKV prev() { return simple(MDB_PREV); }
        MaybeKV prevDup() { return simple(MDB_PREV_DUP); }
        MaybeKV prevNoDup() { return simple(MDB_PREV_NODUP); }

        bool seek(Data key) {
            int rc = mdb_cursor_get(get(), &key, nullptr, MDB_SET);
            if (rc == MDB_NOTFOUND) return false;
            check(rc);
            return true;
        }

        MaybeKV seekKey(Data key) { return simple(MDB_SET_KEY, KV(key, Data())); }
        MaybeKV seekKey(Data key, Data val) { return simple(MDB_GET_BOTH, KV(key, val)); }
        
        MaybeKV seekRange(Data key) { return simple(MDB_SET_RANGE, KV(key, Data())); }
        MaybeKV seekRange(Data key, Data val) { return simple(MDB_GET_BOTH_RANGE, KV(key, val)); }

        //
        // Wrappers around mdb_cursor_put
        //
        // Allowed flags:
        // * MDB_RESERVE (pointer to write to returned)
        // * MDB_NODUPDATA (throws if kv-pair exists)
        // * MDB_NOOVERWRITE (throws if kv-pair exists)
        //
        // Allowed, but be very careful:
        // * MDB_APPEND
        // * MDB_APPENDDUP
        // * MDB_MULTIPLE (val uses a weird format)
        //
        
        KV put(Data key, Data val, unsigned int flags = 0) {
            auto kv = KV{key, val};
            check(mdb_cursor_put(get(), &kv.first, &kv.second, flags));
            return kv;
        }

        Data replaceCurrent(Data val, unsigned int flags = 0) {
            check(mdb_cursor_put(get(), nullptr, &val, MDB_CURRENT | flags));
            return val;
        }

        //
        // others
        //

        void deleteCurrent() { check(mdb_cursor_del(get(), 0)); }
        void deleteCurrentAllDups() { check(mdb_cursor_del(get(), MDB_NODUPDATA)); }

        size_t countDups() {
            size_t out;
            check(mdb_cursor_count(get(), &out));
            return out;
        }

        MDB_cursor* get() { return _cursor.get(); }
        /*implicit*/ operator MDB_cursor* () { return get(); }

    private:
        // BE CAREFUL WITH const
        MaybeKV simple(MDB_cursor_op op, KV kv = KV()) const{
            int rc = mdb_cursor_get(get(), &kv.first, &kv.second, op);
            if (rc == MDB_NOTFOUND)
                return boost::none;
            check(rc);
            return kv;
        }

        MDB_cursor* get() const { return _cursor.get(); }
        unique_ptr_with_deleter<MDB_cursor, mdb_cursor_close> _cursor;
    };
}
}

// TODO new header?
#include "mongo/base/string_data.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/structure/btree/key.h"
#include "mongo/db/diskloc.h"

namespace mongo {
    class Record;

namespace mdb {
    template <>
    struct Adapter<BSONObj> {
        Data toMDB(const BSONObj& obj) { return Data(obj.objsize(), obj.objdata()); }
        BSONObj fromMDB(const Data& data) {
            auto obj = BSONObj(static_cast<char*>(data.mv_data));
            invariant(size_t(obj.objsize()) == data.mv_size);
            return obj;
        }
    };

    template <>
    struct Adapter<StringData> {
        Data toMDB(const StringData& str) { return Data(str.size(), str.rawData()); }
        StringData fromMDB(const Data& data) {
            return StringData(static_cast<char*>(data.mv_data), data.mv_size);
        }
    };

    template <>
    struct Adapter<std::string> {
        Data toMDB(const std::string& str) { return Data(str.size(), str.data()); }
        std::string fromMDB(const Data& data) {
            return std::string(static_cast<char*>(data.mv_data), data.mv_size);
        }
    };

    // string literals only!
    template <size_t N>
    struct Adapter<char[N]> {
        Data toMDB(const char str[N]) { return Data(N, str); }
        void fromMDB(const Data& data) = delete; // not for buffers
    };

    template <>
    struct Adapter<uint32_t> {
        Data toMDB(const uint32_t& obj) { return Data(sizeof(obj), &obj); }
        uint32_t fromMDB(const Data& data) {
            invariant(data.mv_size == sizeof(uint32_t));
            uint32_t out;
            memcpy(&out, data.mv_data, sizeof(out));
            return out;
        }
    };

    template <>
    struct Adapter<uint64_t> {
        Data toMDB(const uint64_t& obj) {return Data(sizeof(obj), &obj); }
        uint64_t fromMDB(const Data& data) {
            invariant(data.mv_size == sizeof(uint64_t));
            uint64_t out;
            memcpy(&out, data.mv_data, sizeof(out));
            return out;
        }
    };

    template<>
    struct Adapter<Record*> {
        Data toMDB(const Record*& obj) = delete;
        Record* fromMDB(const Data& data) {
            // this is a hack, but needed for compat for now
            return reinterpret_cast<Record*>(reinterpret_cast<char*>(data.mv_data) - 16);
        }
    };

    template <>
    struct Adapter<KeyV1> {
        Data toMDB(const KeyV1& key) { return Data(key.dataSize(), key.data()); }
        KeyV1 fromMDB(const Data& data) {
            auto key = KeyV1(static_cast<char*>(data.mv_data));
            dassert(size_t(key.dataSize()) == data.mv_size);
            return key;
        }
    };

    template <>
    struct Adapter<KeyV1Owned> {
        Data toMDB(const KeyV1Owned& key) { return Data(key.dataSize(), key.data()); }
        KeyV1Owned fromMDB(const Data& data) = delete;
    };

    template <>
    struct Adapter<DiskLoc> {
        Data toMDB(const DiskLoc& loc) {return Data(sizeof(loc), &loc); }
        DiskLoc fromMDB(const Data& data) {
            invariant(data.mv_size == sizeof(DiskLoc));
            DiskLoc out;
            memcpy(&out, data.mv_data, sizeof(out));
            return out;
        }
    };
}
}
