//
// Created by Ap0l1o on 2022/3/28.
//

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"
#include "leveldb/options.h"

namespace leveldb {

    // 作为插入缓存的KV项中的Value
    struct TableAndFile {
        RandomAccessFile* file;
        Table* table;
    };

    static void DeleteEntry(const Slice& key, void* value) {
        TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
        delete tf->table;
        delete tf->file;
        delete tf;
    }

    static void UnrefEntry(void* arg1, void* arg2) {
        Cache* cache = reinterpret_cast<Cache*>(arg1);
        Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
        cache->Release(h);
    }

    TableCache::TableCache(const std::string &dbname, const Options &options, int entries)
        : env_(options.env),
          dbname_(dbname),
          options_(options),
          cache_(NewLRUCache(entries)){}


    TableCache::~TableCache() { delete cache_; }

    // 该方法用于查找table，先在缓存中查找，缓存中没有再打开文件并加载到缓存
    Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle **handle) {
        Status s;
        // 将file_number编码为key
        char buf[sizeof(file_number)];
        EncodeFixed64(buf, file_number);
        Slice key(buf, sizeof(buf));
        // 在缓存中查找
        *handle = cache_->Lookup(key);
        // 未在缓存中查到
        if(*handle == nullptr) {
            // 获取文件名
            std::string fname = TableFileName(dbname_, file_number);
            RandomAccessFile* file = nullptr;
            Table* table = nullptr;
            // 打开文件
            s = env_->NewRandomAccessFile(fname, &file);
            // 出错了，则找旧版本的数据
            if(!s.ok()) {
                std::string old_fname = SSTTableFileName(dbname_, file_number);
                if(env_->NewRandomAccessFile(old_fname, &file).ok()) {
                    s = Status::OK();
                }
            }

            // 打开SSTable文件
            if(s.ok()) {
                s = Table::Open(options_, file, file_size, &table);
            }

            if(!s.ok()) {
                assert(table == nullptr);
                delete file;
            } else {
                // 作为键值对将table信息插入TableCache缓存，其中key是编码后的file_number, value是
                // TableAndFile对象
                TableAndFile* tf = new TableAndFile;
                tf->file = file;
                tf->table = table;
                // Insert会返回插入缓存的节点（也就是handle指针）
                *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
            }
        }

        return s;
    }

    // 根据指定的文件(file_number标识的文件)返回一个迭代器，文件大小为file_size，单位为字节，如果
    // tableptr不是nullptr，则其指向返回的迭代器的底层table指针，返回的tableptr指针归缓存所有，不能被删除。
    Iterator* TableCache::NewIterator(const ReadOptions &options, uint64_t file_number, uint64_t file_size,
                                      Table **tableptr) {
        if(tableptr != nullptr) {
            *tableptr = nullptr;
        }

        Cache::Handle* handle = nullptr;
        // 查找table
        Status s = FindTable(file_number, file_size, &handle);
        // 没有找到table，返回错误
        if(!s.ok()) {
            return NewErrorIterator(s);
        }

        // 先从缓存中取出Value（这里的Value是TableAndFile），然后从Value中取出table
        Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
        // 然后获取遍历table迭代器
        Iterator* result = table->NewIterator(options);
        // 注册清理函数
        result->RegisterCleanup(&UnrefEntry, cache_, handle);

        if(tableptr != nullptr) {
            *tableptr = table;
        }

        return result;
    }

    // 如果在指定的文件中根据internal key（也就是参数中的k）找到了一个对应项，则调用
    // (*handle_result)(void*, const Slice&, const Slice&)。
    Status TableCache::Get(const ReadOptions &options, uint64_t file_number, uint64_t file_size, const Slice &k,
                           void *arg, void (*handle_result)(void *, const Slice &, const Slice &)) {

        Cache::Handle* handle = nullptr;
        // 找table
        Status s = FindTable(file_number, file_size, &handle);
        // 找到table（找到后会加载到缓存）
        if(s.ok()) {
            Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
            // 根据key查找，查找到后调用handle_result函数
            s = t->InternalGet(options, k, arg, handle_result);
            // 找到table后就可以释放缓存节点了
            cache_->Release(handle);
        }

        return s;
    }

    void TableCache::Evict(uint64_t file_number) {
        // 根据file_number构造key
        char buf[sizeof(file_number)];
        EncodeFixed64(buf, file_number);
        cache_->Erase(Slice(buf, sizeof(buf)));
    }

} // end namespace leveldb