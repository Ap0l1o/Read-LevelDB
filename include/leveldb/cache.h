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
    // 创建Cache的全局方法
    LEVELDB_EXPORT Cache* NewLRUCache(size_t capacity);

    class LEVELDB_EXPORT Cache {
    public:
        Cache() = default;
        Cache(const Cache&) = delete;
        Cache& operator=(const Cache&) = delete;
        virtual ~Cache();

        // 表示节点的结构体
        struct Handle {};

        // 插入KV对，指定占用的缓存大小（charge），并返回插入的节点。
        // 删除KV对时调用传入deleter函数。
        virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                               void (*deleter)(const Slice& key, void* value)) = 0;

        // 根据key查找数据，返回查找到的节点，若未找到
        // 则返回nullptr
        virtual Handle* Lookup(const Slice& key) = 0;

        // 释放上次Lookup()函数所返回的节点
        virtual void Release(Handle* handle) = 0;

        // 返回节点中存储的value
        virtual void* Value(Handle* handle) = 0;

        // 若缓存包含此key所对应的缓存项，则删除包含key的节点
        virtual void Erase(const Slice& key) = 0;

        // 分配一个新的id并返回
        virtual uint64_t NewId() = 0;

        // 删除所有未在使用中的缓存条目。内存受限的应用程序可能会调用此方法来
        // 减少内存占用。
        virtual void Prune() {}

        // 返回缓存中所有元素的总占用空间的估计值
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
