//
// Created by Ap0l1o on 2022/3/17.
//

#ifndef LLEVELDB_FILTER_BLOCK_H
#define LLEVELDB_FILTER_BLOCK_H

#include <cstddef>
#include <cstdint>
#include <vector>
#include <string>

#include "leveldb/slice.h"
#include "util/hash.h"

// filter block内存储的数据可以分为两部分：过滤数据 和 索引数据
// 索引数据包括：
//              1. filter i offset ， 也即第i个filter的位置偏移
//              2. filter offset's offset, 也即filter的索引的位置偏移

namespace leveldb {
    class FilterPolicy;
    // 一个SSTable只有一个filter block，其内存储了所有block的filter数据。
    // FilterBlockBuilder用于为一个SSTable构建其所有的filter，每个filter是一个string，
    // FilterPolicy会将其存储在filter block 中
    class FilterBlockBuilder {
    public:
        explicit FilterBlockBuilder(const FilterPolicy* );
        FilterBlockBuilder(const FilterBlockBuilder& ) = delete;
        FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

        void StartBlock(uint64_t block_offset);
        void AddKey(const Slice& key);
        Slice Finish();

    private:
        void GenerateFilter();

        const FilterPolicy* policy_;
        // 用于构建Filter的所有key
        std::string keys_;
        // keys_中每个key的起始位置，也即其位置偏移
        std::vector<size_t> start_;
        // 计算得到的filter
        std::string result_;
        // keys_的副本，用于生成filter
        std::vector<Slice> tmp_keys_;
        // filter block 中每个filter的位置偏移
        std::vector<uint32_t> filter_offsets_;
    };

    class FilterBlockReader {
    public:
        // 当前对象存在期间，contents和policy必须保持有效
        FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
        bool KeyMayMatch(uint64_t block_offset, const Slice& key);

    private:
        const FilterPolicy* policy_;
        // 指向filter block中第一个filter的位置
        const char* data_;
        // 每个索引数据指向一个filter的位置偏移
        // offset_ 即filter block的索引数据在filter block中的起始偏移
        const char* offset_;
        // filter的数量，也即filter's index 的数量
        size_t num_;
        // 表示每隔多少数据创建一个新的过滤器来存放过滤数据
        // 默认值为11，表示每2KB（2^11 = 2K）的数据，创建一个新的过滤器来存放过滤数据
        size_t base_lg_;
    };
} // end namespace leveldb


#endif //LLEVELDB_FILTER_BLOCK_H
