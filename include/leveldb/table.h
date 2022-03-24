//
// Created by Ap0l1o on 2022/3/24.
//

#ifndef LLEVELDB_TABLE_H
#define LLEVELDB_TABLE_H

#include "leveldb/export.h"
#include "leveldb/iterator.h"

namespace leveldb {
    class Block;
    class BlockHandle;
    class Footer;
    class Options;
    class RandomAccessFile;
    struct ReadOptions;
    class TableCache;


    // 一个SSTable是一个有序的string->string映射。
    // SSTable是不可变且持久到磁盘上的。
    // SSTable可以在不经过额外并发控制的情况下被多个线程安全访问。
    class LEVELDB_EXPORT Table {
    public:
        // 尝试打开一个Table，其文件大小为file_size，并且会读取一些元数据以用于遍历Table。
        // 成功：返回ok，并且设置*table使其指向打开的文件。当不再继续读取的时候应该delete *file
        // 失败：设置*table指向nullptr，返回错误信息。
        static Status Open(const Options options, RandomAccessFile* file, uint64_t file_size, Table** table);

        Table(const Table&) = delete;
        Table& operator=(const Table&) = delete;
        ~Table();

        // 返回一个遍历table内容的迭代器。
        Iterator* NewIterator(const ReadOptions&) const;

        // 获取目标key对应的数据在Table中的位置偏移
        uint64_t ApproximateOffset(const Slice& key) const;

    private:
        friend class TableCache;
        struct Rep;

        static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);
        explicit  Table(Rep* rep) :rep_(rep) {};

        Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                           void (*handle_result)(void* arg, const Slice& k,
                                                const Slice& v));

        void ReadMeta(const Footer& footer);
        void ReadFilter(const Slice& filter_handle_value);


        Rep* rep_;
    };

} // end namespace leveldb


#endif //LLEVELDB_TABLE_H
