//
// Created by Ap0l1o on 2022/3/23.
//

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
    struct TableBuilder::Rep {
        Rep(const Options& opt, WritableFile* f)
            :   options(opt),
                index_block_options(opt),
                file(f),
                offset(0),
                data_block(&options),
                index_block(&index_block_options),
                num_entries(0),
                closed(false),
                filter_block(opt.filter_policy == nullptr
                                    ? nullptr
                                    : new FilterBlockBuilder(opt.filter_policy)),
                pending_index_entry(false) {

            index_block_options.block_restart_interval = 1;
        }

        // 当前data block的option
        Options options;
        // SSTable的index block 的 option
        Options index_block_options;
        // SSTable文件
        WritableFile* file;
        // 要写入的data block 在SSTable中的位置偏移，初始为0
        uint64_t offset;
        Status status;
        // 当前操作的data block
        BlockBuilder data_block;
        // 当前SSTable的index block
        BlockBuilder index_block;
        // 当前data block的最后一个key
        std::string last_key;
        // 当前SSTable中的key 的个数
        int64_t  num_entries;
        // 是否已经调用过Finish()或Abandon()
        bool closed;
        // SSTable中的 filter block
        FilterBlockBuilder* filter_block;

        // 直到看到下一个data block的第一个key时才生成当前block的index
        // 只有当data block为空时，pending_index_entry才为true
        bool pending_index_entry;
        // 待加入到index block 的handle，也即当前data block的handle
        BlockHandle pending_handle;
        // 临时存储压缩后的data block
        std::string compressed_output;
    };

    TableBuilder::TableBuilder(const Options& options, WritableFile* file)
            : rep_(new Rep(options, file)) {
        if(rep_->filter_block != nullptr) {
            rep_->filter_block->StartBlock(0);
        }
    }

    TableBuilder::~TableBuilder() {
        assert(rep_->closed);
        delete rep_->filter_block;
        delete rep_;
    }

    Status TableBuilder::ChangeOptions(const Options &options) {
        if(options.comparator != rep_->options.comparator) {
            return Status::InvalidArgument("changing comparator while building table");
        }

        rep_->options = options;
        rep_->index_block_options = options;
        rep_->index_block_options.block_restart_interval = 1;
        return Status::OK();
    }

    // 向SSTable中写数据，实际写顺序是先向data block中写数据，
    // data block写满后再刷新到SSTabel
    void TableBuilder::Add(const Slice &key, const Slice &value) {
        Rep* r = rep_;

        assert(!r->closed);
        if(!ok()) return;
        // 保证有序
        if(r->num_entries > 0) {
            assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
        }
        // 表示遇到新的data block ，此key为data block的第一个key
        if(r->pending_index_entry) {
            // 确定确实是新的data block，也即其为空
            assert(r->data_block.empty());
            // 调整last_key
            r->options.comparator->FindShortestSeparator(&r->last_key, key);
            std::string handle_encoding;
            // 将data block的handle压缩存储到handle_encoding
            r->pending_handle.EncodeTo(&handle_encoding);
            // 将data block的handle信息存到index block
            r->index_block.Add(r->last_key, Slice(handle_encoding));
            r->pending_index_entry = false;
        }

        // 将key添加到filter block
        if(r->filter_block != nullptr) {
            r->filter_block->AddKey(key);
        }

        // 调整更新last_key
        r->last_key.assign(key.data(), key.size());
        r->num_entries++;
        r->data_block.Add(key, value);
        // 获取当前 data block的大小
        const size_t  estimated_block_size = r->data_block.CurrentSizeEstimate();
        // 若data block 已写满，则刷新到SSTable
        if(estimated_block_size > r->options.block_size) {
            Flush();
        }
    }

    // 将已经写满的data block刷新到SSTable
    void TableBuilder::Flush() {
        Rep* r = rep_;

        assert(!r->closed);
        if(!ok()) return;
        if(r->data_block.empty()) return;
        assert(!r->pending_index_entry);

        // 将block写到SSTable
        WriteBlock(&r->data_block, &r->pending_handle);

        if(ok()) {
            // 成功将data block写入SSTable后，新的kv数据会写入新的data block
            // 所以，将pending_index_entry置为true，表示开始写一个新的data block
            r->pending_index_entry = true;
            r->status = r->file->Flush();
        }

        if(r->filter_block != nullptr) {
            // 生成布隆过滤器
            r->filter_block->StartBlock(r->offset);
        }
    }

    // 对block数据进行最终处理（压缩），然后调用WriteRawBlock函数写入SSTable
    void TableBuilder::WriteBlock(BlockBuilder *block, BlockHandle *handle) {
        // File包含一系列的block ，每个block由以下部分组成：
        //    block_data: uint8[n]
        //    type: uint8
        //    crc: uint32

        assert(ok());
        Rep* r = rep_;
        // 获取完整的完成写入的block数据
        Slice raw = block->Finish();

        Slice block_contents;
        CompressionType type = r->options.compression;
        switch (type) {
            case kNoCompression:
                block_contents = raw;
                break;

            case kSnappyCompression:
                std::string* compressed = &r->compressed_output;
                if(port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
                    compressed->size() < raw.size() - (raw.size() / 8u)) {
                    block_contents = *compressed;
                } else {
                    block_contents = raw;
                    type = kNoCompression;
                }
                break;

        }

        WriteRawBlock(block_contents, type, handle);
        r->compressed_output.clear();
        block->Reset();
    }

    // 将处理完成的block数据写入SSTable
    void TableBuilder::WriteRawBlock(const Slice &block_contents, CompressionType type, BlockHandle *handle) {
        Rep* r = rep_;
        // 设置block的位置偏移
        handle->set_offset(r->offset);
        // 设置block的实际大小
        handle->set_size(block_contents.size());
        // 写入file
        r->status = r->file->Append(block_contents);
        if(r->status.ok()) {
            // 写入1个字节的block type和4个字节的block crc32

            // 分配空间
            char trailer[kBlockTrailerSize];
            // 写入type
            trailer[0] = type;
            // 计算crc32
            uint32_t  crc = crc32c::Value(block_contents.data(), block_contents.size());
            // 对crc32编码
            EncodeFixed32(trailer + 1, crc32c::Mask(crc));
            // 写入file
            r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
            // 成功写入后调整SSTable的当前位置偏移
            if(r->status.ok()) {
                r->offset += block_contents.size() + kBlockTrailerSize;
            }
        }
    }

    Status TableBuilder::status() const { return rep_->status; }

    Status TableBuilder::Finish() {
        Rep* r = rep_;
        Flush();

        assert(!r->closed);
        // 设置标识位，禁止后续写入
        r->closed = true;

        BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;
        //按顺序依次写入filter block -> meta index block -> index block -> footer

        // 写入filter block
        if(ok() && r->filter_block != nullptr) {
            // 写入filter block，并获取其handle，也即filter_block_handle
            // filter block 不需要其他处理，直接调用WriteRawBlock写入即可
            WriteRawBlock(r->filter_block->Finish(), kNoCompression, &filter_block_handle);
        }
        // 写入meta index block
        // meta index block 存的是 filter.name -> filter handle的映射
        if(ok()) {
            // 将该SSTable的options构建一个空的meta index block
            BlockBuilder meta_index_block(&r->options);
            if(r->filter_block != nullptr) {
                // 加入"filter.Name"到filter data位置的映射，本映射便为meta index block的内容
                // 构造key
                std::string key = "filter.";
                key.append(r->options.filter_policy->Name());
                // 构造value
                std::string handle_encoding;
                filter_block_handle.EncodeTo(&handle_encoding);
                // 写入meta index block
                meta_index_block.Add(key, handle_encoding);
            }
            // meta index block还需进一步处理，调用WriteBlock函数写入
            WriteBlock(&meta_index_block, &metaindex_block_handle);
        }
        // 写入index block
        // index block 存的是 data block's last key -> data block handle 的映射
        if(ok()) {
            // 将最后一个data block的信息存入index block
            if(r->pending_index_entry) {
                r->options.comparator->FindShortSuccessor(&r->last_key);
                std::string handle_encoding;
                r->pending_handle.EncodeTo(&handle_encoding);
                r->index_block.Add(r->last_key, Slice(handle_encoding));
                r->pending_index_entry = false;
            }
            WriteBlock(&r->index_block, &index_block_handle);
        }
        // 写入footer
        // footer存了两个重要的block handle，也即meta index block handle 和 index block handle
        if(ok()) {
            Footer footer;
            footer.set_metaidnex_handle(metaindex_block_handle);
            footer.set_index_handle(index_block_handle);
            std::string footer_encoding;
            footer.EncodeTo(&footer_encoding);
            r->status = r->file->Append(footer_encoding);
            if(r->status.ok()) {
                r->offset += footer_encoding.size();
            }
        }
    }

    void TableBuilder::Abandon() {
        Rep* r = rep_;
        assert(!r->closed);
        r->closed = true;
    }

    uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

    // 最后总的offset的大小也即SSTable的大小
    uint64_t TableBuilder::FileSize() const { return rep_->offset; }

} // end namespace leveldb
