//
// Created by Ap0l1o on 2022/3/28.
//

#ifndef LLEVELDB_TABLE_CACHE_H
#define LLEVELDB_TABLE_CACHE_H

#include <cstdint>
#include <cstring>

#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"
#include "port/port.h"

namespace leveldb {

    class Env;

    class TableCache {
    public:
        TableCache(const std::string& dbname, const Options& options, int entries);
        ~TableCache();

        // 根据指定的文件(file_number标识的文件)返回一个迭代器，文件大小为file_size，单位为字节，如果
        // tableptr不是nullptr，则其指向返回的迭代器的底层table指针，返回的tableptr指针归缓存所有，不能被删除。
        Iterator* NewIterator(const ReadOptions& options, uint64_t file_number, uint64_t file_size, Table** tableptr = nullptr);

        // 如果在指定的文件中根据internal key（也就是参数中的k）找到了一个对应项，则调用
        // (*handle_result)(void*, const Slice&, const Slice&)。
        Status Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size, const Slice& k, void * arg,
                   void (*handle_result)(void*, const Slice&, const Slice&));

        // 根据file_number删除缓存项
        void Evict(uint64_t file_number);

    private:
        // 查找table，先在缓存中查找，缓存没有则再去打开table文件，并加载到缓存，并保存
        // 缓存节点到Handle
        Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);
        // 用于文件操作
        Env* const env_;
        // 数据库的名称
        const std::string dbname_;
        // 操作选项配置
        const Options& options_;
        // 使用的缓存对象，TableCache使用了一个ShardedLRUCache,
        // LRUCache缓存的也是KV映射，其中Key是文件编号file_number, Value是TableAndFile
        Cache* cache_;
    };

} // end namespace leveldb

#endif //LLEVELDB_TABLE_CACHE_H
