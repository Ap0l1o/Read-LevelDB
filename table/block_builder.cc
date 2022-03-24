// 1. 在BlockBuilder所构建的block中，key是经过前缀压缩的；
// 2. 当存储一个key时，会丢弃与前面的key所共享的前缀字符串；
// 
// 3. 此外，每间隔K个key，就设置一个未前缀压缩的key，称之为 "restart key" 也即重启点；
// 4. block 的尾部存储了所有重启点的位置偏移，以用来执行二分查找；
// 5. value的存储是不经过压缩的；

// 一个完整的key-value pari 由以下各个部分组成：
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// 
// shared_bytes == 0 for restart points. 重启点的共享字节为0
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts] 保存所有重启点
//     num_restarts: uint32 重启点的数量
// restarts[i] contains the offset within the block of the ith restart point.
// restarts[i] 保存了第i个重启点的位置偏移

#include "table/block_builder.h"
#include <algorithm>
#include <cassert>
#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {
    BlockBuilder::BlockBuilder(const Options* options)
        : options_(options), restarts_(), counter_(0), finished_(false) {
        
        assert(options->block_restart_interval >= 1);
        // 总是将第一个key-value设置为重启点，
        // 因此第一个重启点的位置偏移为0
        restarts_.push_back(0);
    }

    void BlockBuilder::Reset() {
        buffer_.clear();
        restarts_.clear();
        restarts_.push_back(0);
        counter_ = 0;
        finished_ = false;
        last_key_.clear();
    }

    size_t BlockBuilder::CurrentSizeEstimate() const {
        return ( buffer_.size() +
                 restarts_.size() + sizeof(uint32_t) +
                 sizeof(uint32_t) );
    }

    Slice BlockBuilder::Finish() {
        // 将所有重启点的位置偏移添加到buffer_
        for(size_t i=0; i<restarts_.size(); i++) {
            PutFixed32(&buffer_, restarts_[i]);
        }
        // 将重启点的数量添加到buffer_
        PutFixed32(&buffer_, restarts_.size());
        // 谁设置结束标记
        finished_ = true;
        return Slice(buffer_);
    }

    void BlockBuilder::Add(const Slice& key, const Slice& value) {
        
        // 获取上一个添加的key，用于来和当前key比较，以确认当前key大于上一个key
        Slice last_key_piece(last_key_);

        // 是否未结束
        assert(!finished_);
        // 是否counter_满足配置要求
        assert(counter_ <= options_->block_restart_interval);
        // 是否有空间或当前key大于上一个key
        assert(buffer_.empty() || 
               options_->comparator->Compare(key, last_key_piece) > 0);

        // 用于计算key的共享前缀长度
        size_t shared = 0;
        // 不需要设置重启点, 此时需要计算共享长度
        if(counter_ < options_->block_restart_interval) {
            // 获取两者长度中较小的长度
            const size_t min_length = std::min(last_key_piece.size(), key.size());
            while( (shared < min_length) && (last_key_piece[shared] == key[shared]) ) {
                shared++;
            }
        } else {
            // 需要设置重启点
            // 将buffer_的当前位置插入重启点位置偏移数组
            restarts_.push_back(buffer_.size());
            // 重新开始计数
            counter_ = 0;
        }

        // 非共享字节数
        const size_t non_shared = key.size() - shared;
        // 将共享前缀长度 非共享长度 value数据长度依次添加到buffer_
        PutVarint32(&buffer_, shared);
        PutVarint32(&buffer_, non_shared);
        PutVarint32(&buffer_, value.size());
        // 将非共享部分数据加入buffer
        buffer_.append(key.data() + shared, non_shared);
        // 将value加入buffer
        buffer_.append(value.data(), value.size());
        
        // 将上一个key更新为当前key
        last_key_.resize(shared);
        last_key_.append(key.data() + shared, non_shared);
        assert(Slice(last_key_) == key);
        counter_++;

    }

} // end namespace leveldb