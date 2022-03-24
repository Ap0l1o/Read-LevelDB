//
// Created by Ap0l1o on 2022/3/24.
//

#ifndef LLEVELDB_CACHE_H
#define LLEVELDB_CACHE_H

#include <cstdint>
#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {

    class LEVELDB_EXPORT Cache;
    LEVELDB_EXPORT Cache* NewLRUCache(size_t capacity);

    class LEVELDB_EXPORT Cache {
    public:
        Cache() = default;
        Cache(const Cache&) = delete;
        Cache& operator=(const Cache&) = delete;
        virtual ~Cache();

        struct Handle {};

        virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                               void (*deleter)(const Slice& key, void* value)) = 0;

        virtual Handle* Lookup(const Slice& key) = 0;

        virtual void Release(Handle* handle) = 0;

        virtual void* Value(Handle* handle) = 0;

        virtual uint64_t NewId() = 0;

        virtual void Prune() {}

        virtual size_t TotalCharge() const = 0;

    private:
        void LRU_Remove(Handle* e);
        void LRU_Append(Handle* e);
        void Unref(Handle* e);

        struct Rep;
        Rep* rep_;
    };

} // end namespace leveldb

#endif //LLEVELDB_CACHE_H
