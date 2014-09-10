// wiredtiger_database_catalog_entry.h

/**
*    Copyright (C) 2014 MongoDB Inc.
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

#pragma once

#include <list>
#include <map>
#include <string>

#include <boost/thread/mutex.hpp>

#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/catalog/database_catalog_entry.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"

namespace mongo {

    class WiredTigerDatabaseCatalogEntry : public DatabaseCatalogEntry {
    public:
        WiredTigerDatabaseCatalogEntry( WiredTigerDatabase &db, const StringData& name );

        virtual ~WiredTigerDatabaseCatalogEntry();

        virtual bool exists() const { return true; }
        virtual bool isEmpty() const;

        virtual void appendExtraStats( OperationContext* txn,
                                       BSONObjBuilder* out,
                                       double scale ) const;

        // these are hacks :(
        virtual bool isOlderThan24( OperationContext* txn ) const { return false; }
        virtual void markIndexSafe24AndUp( OperationContext* txn ) { }

        /**
         * @return true if current files on disk are compatibile with the current version.
         *              if we return false, then an upgrade will be required
         */
        virtual bool currentFilesCompatible( OperationContext* txn ) const { return true; }

        // ----

        virtual void getCollectionNamespaces( std::list<std::string>* out ) const;

        virtual CollectionCatalogEntry* getCollectionCatalogEntry( OperationContext* txn,
                                                                   const StringData& ns ) const;

        virtual RecordStore* getRecordStore( OperationContext* txn,
                                             const StringData& ns );


        virtual IndexAccessMethod* getIndex( OperationContext* txn,
                                             const CollectionCatalogEntry* collection,
                                             IndexCatalogEntry* index );

        virtual Status createCollection( OperationContext* txn,
                                         const StringData& ns,
                                         const CollectionOptions& options,
                                         bool allocateDefaultSpace );

        virtual Status renameCollection( OperationContext* txn,
                                         const StringData& fromNS,
                                         const StringData& toNS,
                                         bool stayTemp );

        virtual Status dropCollection( OperationContext* txn,
                                       const StringData& ns );

        // Called by the engine when dropping a database.
        Status dropAllCollections( OperationContext* txn );

    private:

        BSONObj getSavedMetadata( WiredTigerCursor &cursor );
        void _loadAllCollections();
        void _loadCollection( WiredTigerSession& swrap,
                              const std::string &tbl_uri,
                              bool stayTemp = false );
        struct IndexEntry {
            std::string name;
            BSONObj spec;
            DiskLoc head;
            bool ready;
            bool isMultikey;
            
            // Only one of these will be in use. See getIndex() implementation.
            scoped_ptr<RecordStore> rs;
            shared_ptr<void> data;
        };

        class Entry : public CollectionCatalogEntry {
        public:
            Entry( const StringData& ns, const CollectionOptions& options );
            virtual ~Entry();

            int getTotalIndexCount( OperationContext* txn ) const;

            int getCompletedIndexCount( OperationContext* txn ) const;

            int getMaxAllowedIndexes() const { return 64; }

            void getAllIndexes( OperationContext* txn, std::vector<std::string>* names ) const;

            BSONObj getIndexSpec( OperationContext* txn, const StringData& idxName ) const;

            bool isIndexMultikey( OperationContext* txn, const StringData& indexName) const;

            bool setIndexIsMultikey(OperationContext* txn,
                                    const StringData& indexName,
                                    bool multikey = true);

            DiskLoc getIndexHead( OperationContext* txn, const StringData& indexName ) const;

            void setIndexHead( OperationContext* txn,
                               const StringData& indexName,
                               const DiskLoc& newHead );

            bool isIndexReady( OperationContext* txn, const StringData& indexName ) const;

            Status removeIndex( OperationContext* txn,
                                const StringData& indexName );

            Status prepareForIndexBuild( OperationContext* txn,
                                         const IndexDescriptor* spec );

            void indexBuildSuccess( OperationContext* txn,
                                    const StringData& indexName );

            void updateTTLSetting( OperationContext* txn,
                                   const StringData& idxName,
                                   long long newExpireSeconds );

            CollectionOptions getCollectionOptions(OperationContext* txn) const { return options; }

            CollectionOptions options;
            scoped_ptr<WiredTigerRecordStore> rs;

            typedef std::map<std::string,IndexEntry*> Indexes;
            Indexes indexes;
        };

        WiredTigerDatabase &_db;

        mutable boost::mutex _entryMapLock;
        typedef std::map<std::string, Entry *> EntryMap;
        EntryMap _entryMap;
    };
}
