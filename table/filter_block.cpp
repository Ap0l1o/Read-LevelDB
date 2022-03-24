//
// Created by Ap0l1o on 2022/3/17.
//

#include "table/filter_block.h"
#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {
    // 表示每隔多少数据创建一个新的过滤器来存放过滤数据
    // 默认值为11，表示每2KB（2^11 = 2K）的数据，创建一个新的过滤器来存放过滤数据
    static const size_t kFilterBaseLg = 11;
    // 1 << 11 = 2048 = 2KB
    static const size_t kFilterBase = 1 << kFilterBaseLg;

    FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy) : policy_(policy) {}

    void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
        // 每2KB数据生成一个Filter，计算需要生成几个Filter
        uint64_t filter_index = (block_offset / kFilterBase);
        assert(filter_index >= filter_offsets_.size());
        // 每2KB数据生成一个Filter
        while(filter_index > filter_offsets_.size()) {
            GenerateFilter();
        }
    }

    void FilterBlockBuilder::AddKey(const Slice &key) {
        Slice k = key;
        // 将该key的位置偏移，也即keys_的当前大小，存入start_
        start_.push_back(keys_.size());
        // 将key存入keys_
        keys_.append(k.data(), k.size());
    }

    Slice FilterBlockBuilder::Finish() {
        if(!start_.empty()) {
            GenerateFilter();
        }

        // 需要先将每个filter的位置偏移，也就是filter offset array加入到result，
        // 然后再将第一个filter的offset的位置加入到result

        // 计算当前result的大小，也即得到filter offset的起始位置
        const uint32_t  array_offset = result_.size();
        // 将每个filter的filter offset，也即每个filter的偏移位置加入result
        for(size_t i=0; i<filter_offsets_.size(); i++) {
            PutFixed32(&result_, filter_offsets_[i]);
        }
        // 最后将filter offset的起始位置，也即第一个filter的offset加入到result中
        // 偏移数组的位置占4个字节
        PutFixed32(&result_, array_offset);

        result_.push_back(kFilterBaseLg);
        return Slice(result_);
    }

    void FilterBlockBuilder::GenerateFilter() {
        const size_t num_keys = start_.size();

        if(num_keys == 0) {
            filter_offsets_.push_back(result_.size());
        }

        start_.push_back(keys_.size());
        tmp_keys_.resize(num_keys);
        for(size_t i=0; i<num_keys; i++) {
            const char* base = keys_.data() + start_[i];
            size_t length = start_[i+1] - start_[i];
            tmp_keys_[i] = Slice(base, length);
        }
        // result_ 的当前大小，也即将要存入的filter的起始位置，
        // 也就是当前filter的位置偏移存入filter offset array
        filter_offsets_.push_back(result_.size());
        // 根据过滤策略生成filter，并将其存入result
        policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

        tmp_keys_.clear();
        keys_.clear();
        start_.clear();
    }

    FilterBlockReader::FilterBlockReader(const FilterPolicy *policy, const Slice &contents)
        : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
        // 计算大小
        size_t n = contents.size();
        // 正常不会小于5字节，因为filter偏移数组的位置占4个字节，base_lg占1个字节，这就是5个字节
        if(n < 5) return ;
        // 获取base_lg，其在最后一个字节
        base_lg_ = contents[n-1];
        // 获取filter offset's offset，也即filter偏移数组的位置，其占4个字节，在base_lg之前
        uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
        // 正常，filter偏移数组的位置（偏移）在倒数第五个字节之前
        if(last_word > 5) return;

        data_ = contents.data();
        // 根据偏移计算filter偏移数组的具体位置
        offset_  = data_ + last_word;
        // 计算filter的数量，其数量等于其索引的数量，每个索引占4个字节（Fixed32占4个字节）
        num_ = (n - 5 - last_word) / 4;
    }

    bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice &key) {
        // 计算索引
        uint64_t  index = block_offset >> base_lg_;
        if(index < num_) {
            // 获取该索引所指向的filter的起始位置偏移
            uint32_t start = DecodeFixed32(offset_ + index * 4);
            // 获取该filter的结束位置偏移（加4即可，每个filter offset是占4个字节Fixed32）
            uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
            if(start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
                Slice filter = Slice(data_ + start, limit - start);
                return policy_->KeyMayMatch(key, filter);
            } else if(start == limit) {
                return false;
            }
        }
        return true;
    }

} // end namespace leveldb