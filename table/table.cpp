//
// Created by Ap0l1o on 2022/3/24.
//

#include "leveldb/table.h"
#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
//#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "leveldb/comparator.h"

namespace leveldb {
    struct Table::Rep {
        ~Rep() {
            delete filter;
            delete[] filter_data;
            delete index_block;
        }

        Options options;
        Status status;
        RandomAccessFile* file;
        uint64_t cache_id;
        FilterBlockReader* filter;
        const char* filter_data;

        BlockHandle metaindex_handle;
        Block* index_block;
    };

    // 所谓Open打开SSTable实际就是先打开SSTable对应的文件，然后读出
    // 该SSTable的index block用于后续遍历
    Status Table::Open(const Options options, RandomAccessFile *file, uint64_t size, Table **table) {
        *table = nullptr;
        if(size < Footer::kEncodeLength) {
            return Status::Corruption("file is too short to be an sstable");
        }
        // =====================================  读取 Footer  =====================================
        // 分配空间，用于读取Footer
        char footer_space[Footer::kEncodeLength];
        // footer_space用于构造footer_input
        Slice footer_input;
        // 从文件尾部读取Footer，可以看到file是随机读文件类型
        Status s = file->Read(size - Footer::kEncodeLength, Footer::kEncodeLength, &footer_input, footer_space);

        if(!s.ok()) return s;

        Footer footer;
        // 从footer_input中解析出footer
        s = footer.DecodeFrom(&footer_input);
        if(!s.ok()) return s;

        // ===================================== 根据footer 读取index block
        BlockContents index_block_contents;
        ReadOptions opt;
        if(options.paranoid_checks) {
            opt.verify_checksums = true;
        }
        // 随机读到index block contents
        s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

        if(s.ok()) {
            // 根据读到的index block的内容构造index block
            Block* index_block = new Block(index_block_contents);
            Rep* rep = new Table::Rep;
            rep->options = options;
            rep->file;
            rep->metaindex_handle = footer.metaindex_handle();
            rep->index_block = index_block;
            rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
            rep->filter_data = nullptr;
            rep->filter = nullptr;
            *table = new Table(rep);
            (*table)->ReadMeta(footer);
        }

        return s;

    }

    // 读取meta index block ，其中存了filter block 的 handle
    void Table::ReadMeta(const Footer &footer) {
        // 没有过滤策略则没有继续读的必要
        if(rep_->options.filter_policy == nullptr) {
            return ;
        }

        ReadOptions opt;
        if(rep_->options.paranoid_checks) {
            opt.verify_checksums = true;
        }
        // 读取meta index block contents
        BlockContents contents;
        if(!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
            return ;
        }
        // 根据contents构造block
        Block* meta = new Block(contents);

        Iterator* iter = meta->NewIterator(BytewiseComparator());
        // meta index block是 filte.Name->filter block handle 的映射
        // 构造Key
        std::string key = "filter.";
        key.append(rep_->options.filter_policy->Name());
        iter->Seek(key);
        // 读取value
        if(iter->Valid() && iter->key() == Slice(key)) {
            ReadFilter(iter->value());
        }
        delete iter;
        delete meta;
    }

    // 根据filter block handle读取filter block，并构造一个filter block reader
    void Table::ReadFilter(const Slice &filter_handle_value) {
        Slice v = filter_handle_value;
        BlockHandle filter_handle;
        // 提取filter handle
        if(!filter_handle.DecodeFrom(&v).ok()) {
            return ;
        }

        ReadOptions opt;
        if(rep_->options.paranoid_checks) {
            opt.verify_checksums = true;
        }
        // 读取block contents
        BlockContents block;
        if(!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
            return ;
        }

        // block空间是堆分配的
        if(block.heap_allocated) {
            rep_->filter_data = block.data.data();
        }

        rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
    }

    Table::~Table() { delete rep_; }

    static void DeleteBlock(void* arg, void* ignored) {
        delete reinterpret_cast<Block*>(arg);
    }

    static void DeleteCachedBlock(const Slice& key, void* value) {
        Block* block = reinterpret_cast<Block*>(value);
        delete block;
    }

    static void ReleaseBlock(void* arg, void* h) {
        Cache* cache = reinterpret_cast<Cache*>(arg);
        Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
        cache->Release(handle);
    }

    Iterator* Table::BlockReader(void* arg, const ReadOptions& options, const Slice& index_value) {
        Table* table = reinterpret_cast<Table*>(arg);
        Cache* block_cache = table->rep_->options.block_cache;
        Block* block = nullptr;
        Cache::Handle* cache_handle = nullptr;

        // 获取要读取的 data block 的 handle
        BlockHandle handle;
        Slice input = index_value;
        Status s = handle.DecodeFrom(&input);
        // =======================================    读取 block   ===============================================
        if(s.ok()) {
            BlockContents contents;
            // 为该data block使用特定缓存
            if(block_cache != nullptr) {
                // 构造该data block的 k->v 映射
                char cache_key_buffer[16];
                EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
                EncodeFixed64(cache_key_buffer + 8, handle.offset());
                Slice key(cache_key_buffer, sizeof(cache_key_buffer));
                // 检查缓存中是否已经有该data block
                cache_handle = block_cache->Lookup(key);
                // 缓存中已经有该data block，直接从缓存中读取
                if(cache_handle != nullptr) {
                    block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
                } else {
                    // 缓存中不存在该data block ，从SSTable文件读取
                    s = ReadBlock(table->rep_->file, options, handle, &contents);
                    if(s.ok()) {
                        block = new Block(contents);
                        // 若需要存到缓存，则将刚读取的data block 存到缓存中
                        if(contents.cacheable && options.fill_cache) {
                            cache_handle = block_cache->Insert(key, block, block->size(),
                                                               DeleteCachedBlock);
                        }
                    }
                }
            } else {
                // 不使用缓存，则直接读取SSTable文件
                s = ReadBlock(table->rep_->file, options, handle, &contents);
                if(s.ok()) {
                    block = new Block(contents);
                }
            }
        }
        // ======================================= 构造读取该block 的迭代器 =======================================
        Iterator* iter;
        if(block != nullptr) {
            iter = block->NewIterator(table->rep_->options.comparator);
            if(cache_handle == nullptr) {
                iter->RegisterCleanup(&DeleteBlock, block, nullptr);
            } else {
                iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
            }
        } else {
            iter = NewErrorIterator(s);
        }

        return iter;
    }

} // end namespace leveldb