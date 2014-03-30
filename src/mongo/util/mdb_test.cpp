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

#include "mongo/util/mdb.h"

#include <algorithm>
#include <stdlib.h>

#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"

using namespace std;
using namespace mongo;

constexpr MDB_val operator"" _mval(const char* ptr, size_t size) {
    return MDB_val{size, const_cast<char*>(ptr)};
}

TEST(MDB, Integer) {
    unittest::TempDir dir("mdb");
    
    mdb::Env env;

    env.open((dir.path() + "/data").c_str(), !MDB_NOTLS | MDB_NOSUBDIR | MDB_WRITEMAP);

    auto makeDB = [&env]() -> mdb::DB {
        mdb::DB db;
        auto txn = mdb::Txn::Write(env);
        db.open(txn, "DB", MDB_CREATE | MDB_INTEGERKEY);
        txn.commit();
        return db;
    };

    mdb::DB db = makeDB();

    ASSERT_EQUALS(mdb::Data(uint64_t(1)).as<uint64_t>(), uint64_t(1));
    ASSERT_EQUALS(mdb::Data(uint64_t(2)).as<uint64_t>(), uint64_t(2));

    auto nums = std::vector<uint64_t>();
    nums.reserve(1000*1000);
    for (auto i = uint64_t(0); i < 1000*1000; i++) {
        nums.push_back(i);
    }
    //std::random_shuffle(nums.begin(), nums.end());

    const auto obj = BSON(GENOID <<  "s" << "some string");

    for (auto i = 0; i < 1; i++) {
        {
            auto txn = mdb::Txn::Write(env);
            db.drop(txn);
            txn.commit();

            db = makeDB();
        }
        Timer t;
        for (auto i = uint64_t(0); i < 1000*1000; i++) {
            auto txn = mdb::Txn::Write(env);
            db.put(txn, nums[i], obj);
            txn.commit();
        }
        PRINT(t.millisReset());
    }

    auto info = env.info();
    PRINT(info.me_mapsize);
    PRINT(info.me_last_pgno);
    

    {
        auto txn = mdb::Txn::Read(env);

        auto stats = db.stats(txn);
        PRINT(stats.ms_entries);
        PRINT(stats.ms_depth);
        PRINT(stats.ms_leaf_pages);
        PRINT(stats.ms_branch_pages);
        PRINT(stats.ms_overflow_pages);


        auto t = Timer();
        {
            auto cursor = mdb::Cursor(txn, db);
            int i =0;
            while (auto kv = cursor.nextNoDup()) {
                i++;
                //ASSERT_EQUALS(cursor.countDups(), 1u);
            }
            PRINT(i);
        }
        PRINT(t.millisReset());

        for (auto i = uint64_t(0); i < 1000*1000; i++) {
            db.get(txn, i);
        }
        PRINT(t.millisReset());

        for (auto i = uint64_t(0); i < 1000*1000; i++) {
            txn.reset();
            txn.renew();
            db.get(txn, nums[i]);
        }
        PRINT(t.millisReset());

        std::random_shuffle(nums.begin(), nums.end());
        t.reset();
        for (auto i = uint64_t(0); i < 1000*1000; i++) {
            txn.reset();
            txn.renew();
            db.get(txn, nums[i]);
        }
        PRINT(t.millisReset());


    }
    system(("ls -l " + dir.path()).c_str());
}

TEST(MDB, Simple) {
    unittest::TempDir dir("mdb");
    
    mdb::Env env;

    env.open((dir.path() + "/data").c_str(), MDB_NOTLS | MDB_NOSUBDIR);

    mdb::DB db;
    {
        auto txn = mdb::Txn::Write(env);
        db.open(txn, "DB", MDB_CREATE);
        txn.commit();
    }

    {
        auto txnOuter = mdb::Txn::Write(env);
        {
            auto txn = mdb::Txn::Write(env, txnOuter);
            db.put(txn, "hello", std::string("world"));
            txn.commit();
        }

        try {
            cout << db.get(txnOuter, "hello").as<StringData>() << endl;
        } catch (const mdb::Error& e) {
            if (e.code().value() != MDB_NOTFOUND) {
                throw;
            }
            cout << "NOT_FOUND" << endl;
        }

        txnOuter.commit();

        txnOuter = mdb::Txn::Read(env);

        cout << db.get(txnOuter, "hello").as<StringData>() << endl;
    }
    
    {
        auto txn = mdb::Txn::Read(env);
        auto cursor = mdb::Cursor(txn, db);
        while (auto kv = cursor.next()) {
            cout << kv->first.as<StringData>() << ':' << kv->second.as<StringData>() << endl;
        }
    }
}


