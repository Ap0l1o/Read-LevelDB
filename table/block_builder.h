// 每个block由三部分组成：block data  +  type  + crc32
// block data的构建由BlockBuilder类来实现

#ifndef BLOCK_BUILDER_H_
#define BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>
#include "leveldb/slice.h"

namespace leveldb {

    struct Options;

    class BlockBuilder {
        public:
        explicit BlockBuilder(const Options* options);
        BlockBuilder(const BlockBuilder&) = delete;
        BlockBuilder operator=(const BlockBuilder&) = delete;

        // 复位函数，重置内容，清空各个信息
        void Reset();

        // 添加K/V
        // 要求：1. 调用Reset()后未再调用过Finish(); 
        //      2. 当前key大于任何已添加的key
        void Add(const Slice& key, const Slice& value);

        // 结束构建block，返回指向block内容的指针
        // 在调用Reset()之前，返回的Slice始终有效
        Slice Finish();

        // 返回估计的当前正在构建的block的大小（未压缩）
        size_t CurrentSizeEstimate() const;
        
        // buffer_无内容则返回true 
        bool empty() const { return buffer_.empty(); }
        
        private:
        const Options* options_;
        // 目标buffer，也即block 的内容
        std::string buffer_;
        // 重启点
        std::vector<uint32_t> restarts_;
        // 设置上一个重启点之后新增的key数量，用于判断当前要add的key是否要作为重启点
        int counter_;
        // 记录Finish()函数是否被调用过
        bool finished_;
        // 记录最后添加的key
        std::string last_key_;
    };
} // end namespace leveldb



#endif // BLOCK_BUILDER_H_