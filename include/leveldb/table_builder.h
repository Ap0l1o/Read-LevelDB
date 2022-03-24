//
// Created by Ap0l1o on 2022/3/23.
//

#ifndef LLEVELDB_TABLE_BUILDER_H
#define LLEVELDB_TABLE_BUILDER_H

#include <cstdint>
#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {

    class BlockBuilder;
    class BlockHandle;
    class WritableFile;

    // 创建一个生成器，该生成器将在*file中存储生成的table的内容
    class LEVELDB_EXPORT TableBuilder {
    public:
        TableBuilder(const Options& options, WritableFile* file);
        TableBuilder(const TableBuilder&) = delete;
        TableBuilder& operator=(const TableBuilder&) = delete;

        ~TableBuilder();

        // 修改此Builder所使用的options
        Status ChangeOptions(const Options& options);

        // 向table中添加键值对
        void Add(const Slice& key, const Slice& value);

        // Advanced operation: flush any buffered key/value pairs to file.
        // Can be used to ensure that two adjacent entries never live in
        // the same data block.  Most clients should not need to use this method.
        // REQUIRES: Finish(), Abandon() have not been called
        void Flush();

        // 如果检测到错误产生的话则返回相关信息
        Status status() const;

        // 完成table的构建
        Status Finish();

        // 指示当前生成器所构造的table内容应该丢弃
        void Abandon();

        // 所有键值对的数量，也即调用Add()函数的次数
        uint64_t NumEntries() const;

        // 返回生成的table文件的大小
        uint64_t FileSize() const;
    private:
        bool ok() const { return status().ok(); }
        void WriteBlock(BlockBuilder* block, BlockHandle* handle);
        void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

        struct Rep;
        Rep* rep_;
    };

} // end namespace leveldb


#endif //LLEVELDB_TABLE_BUILDER_H
