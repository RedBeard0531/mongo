/**
 *    Copyright (C) 2014 10gen Inc.
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

#include "mongo/db/structure/record_store_mdb.h"

#include "mongo/db/client.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/dur.h"
#include "mongo/db/storage/extent.h"
#include "mongo/db/storage/extent_manager.h"
#include "mongo/db/storage/record.h"
#include "mongo/db/structure/catalog/namespace_details.h"
#include "mongo/util/mmap.h"

namespace mongo {

    RecordStoreMDB::RecordStoreMDB(const StringData& ns,
                                   NamespaceDetails* details,
                                   mdb::DB& db,
                                   uint32_t dbnum)
        : RecordStore( ns )
        , _details( details )
        , _db(db)
        , _dbNum(dbnum)
    {
        if (NamespaceString::normal(ns)) {
            auto& txn = cc().getContext()->getTxn();
            auto cursor = mdb::Cursor(txn, _db);
            if (auto kv = cursor.last()) {
                _nextId = kv->first.as<uint32_t>() + 1;
            } else {
                _nextId = 0;
            }
        }
    }

    Record* RecordStoreMDB::recordFor( const DiskLoc& loc ) const {
        auto ml = MDBLoc(loc);
        invariant(ml.collection == _dbNum);
        return _db.get(cc().getContext()->getTxn(), ml.id).as<Record*>();
    }

    void RecordStoreMDB::cappedPostInsert() {
        if (!_details->isCapped())
            return;

        if (size_t(_details->dataSize()) <= _details->maxCappedSize()
            && _details->numRecords() <= _details->maxCappedDocs())
            return; // don't init the cursor

        auto& txn = cc().getContext()->getTxn();
        auto cursor = mdb::Cursor(txn, _db);
        while (size_t(_details->dataSize()) > _details->maxCappedSize()
               || _details->numRecords() > _details->maxCappedDocs()) {
            auto kv = cursor.next();

            // this would mean deleting what we just inserted. possible today, but should check
            // before we get here and fail the insert instead.
            invariant(kv);

            _details->incrementStats( -1 * kv->second.mv_size, -1 );
            cursor.deleteCurrent();
        }
    }

    StatusWith<DiskLoc> RecordStoreMDB::insertRecord( const DocWriter* doc, int quotaMax ) {
        auto& txn = cc().getContext()->getTxn();
        auto id = _nextId++;
        const auto size = size_t(doc->documentSize());
        invariant(int(id) <= maxDiskLoc.getOfs());
        auto kv = _db.put(txn, id, mdb::Data(size, nullptr), MDB_RESERVE | MDB_APPEND);
        invariant(kv.second.mv_size == size);
        doc->writeDocument( reinterpret_cast<char*>(kv.second.mv_data) );

        _details->incrementStats( size, 1 );

        cappedPostInsert();

        return StatusWith<DiskLoc>(DiskLoc(MDBLoc(_dbNum, id)));
    }


    StatusWith<DiskLoc> RecordStoreMDB::insertRecord( const char* data, int len, int quotaMax ) {
        auto& txn = cc().getContext()->getTxn();
        auto id = _nextId++;
        invariant(int(id) <= maxDiskLoc.getOfs());
        auto kv = _db.put(txn, id, mdb::Data(len, data), MDB_APPEND);
        invariant(kv.second.mv_size == size_t(len));

        _details->incrementStats( len, 1 );

        cappedPostInsert();

        return StatusWith<DiskLoc>(DiskLoc(MDBLoc(_dbNum, id)));
    }

    void RecordStoreMDB::deleteRecord( const DiskLoc& dl ) {
        auto ml = MDBLoc(dl);
        invariant(ml.collection == _dbNum);

        auto& txn = cc().getContext()->getTxn();
        auto cursor = mdb::Cursor(txn, _db);
        auto kv = cursor.seekKey(ml.id);
        invariant(kv);
        cursor.deleteCurrent();
        _details->incrementStats( -1 * kv->second.mv_size, -1 );
    }

    Status RecordStoreMDB::truncate() {
        auto& txn = cc().getContext()->getTxn();
        _db.empty(txn);
        return Status::OK();
    }
}
