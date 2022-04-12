#include "table/block.h"
#include <algorithm>
#include <cstdint>
#include <vector>

#include "leveldb/comparator.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

    // 从最后4个字节中解析出重启点的数量
    inline uint32_t Block::NumRestarts() const {
        assert(size_ > sizeof(uint32_t));
        return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
    }

    Block::Block(const BlockContents& contents) 
        : data_(contents.data.data()),
          size_(contents.data.size()),
          owned_(contents.heap_allocated) {

        if(size_ < sizeof(uint32_t)) {
            size_ = 0;
        } else {
            // 要留出4个字节来存储重启点的数量，计算最多允许的重启点数量，每个重启点需要4个字节来保存其位置偏移
            size_t max_restarts_allowed = (size_ - sizeof(uint32_t)) / sizeof(uint32_t);
            if(NumRestarts() > max_restarts_allowed) {
                size_= 0;
            } else {
                //计算重启点偏移数组的位置
                // +1 是因为最后4字节是保存的重启点的数量
                restart_offset_ = size_ - (1 + NumRestarts()) * sizeof(uint32_t);
            }
        }
    }

    Block::~Block() {
        if(owned_) {
            delete[] data_;
        }
    }


    // 从指针p开始，从block解析出一个entry中的shared_bytes, non_shared_bytes和value_length，
    // entry的完整格式为：
    // shared_bytes + non_shared_bytes + value_length + delta key + value
    //
    // 若检测到错误，返回 nullptr
    // 否则，返回指向delta key的指针
    static inline const char* DecodeEntry(const char* p, const char* limit, 
                                          uint32_t* shared, uint32_t* non_shared,
                                          uint32_t* value_length) {
        // shared, non_shared和value_length都是varint32
        // 每个都至少占1个字节，加起来就是不少于3个字节
        // 若是少于三个字节则显然不合理
        if(limit - p < 3) return nullptr;

        // 上面也提到了，以上各个参数都至少占1个字节，
        // 因此可能是都只占一个字节的，所以可以都先取1个字节然后再来验证是否真的是只用一个字节来存储的
        *shared = reinterpret_cast<const uint8_t*>(p)[0];
        *non_shared = reinterpret_cast<const uint8_t*>(p)[1];
        *value_length = reinterpret_cast<const uint8_t*>(p)[2];
        // 验证以上3个参数是否真的是只用一个字节来存储的
        if((*shared | *non_shared | *value_length) < 128) {
            // 以上各个参数确实仅用一个字节就完成了存储
            // p指针后移3
            p += 3;
            // 通过以上操作，可以快速读取
        } else {
            // 猜测不成立，需要通过复杂的操作来解析各个参数
            if((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
            if((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) return nullptr;
            if((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) return nullptr;
        }

        if(static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)) {
            return nullptr;
        }

        return p;

    }

    class Block::Iter : public Iterator {

        private:

        const Comparator* const comparator_;
        const char* const data_; // 当下block的内容
        uint32_t const restarts_; // 重启点偏移数组的位置偏移
        uint32_t const num_restarts_; // 重启点的数量

        // current_是当前entry的位置偏移，正常情况下 < restarts_
        uint32_t current_;
        // current_所在的重启点的index
        uint32_t restart_index_;
        // 当前位置的key和value
        std::string key_;
        Slice value_;
        Status status_;

        inline int Compare(const Slice& a, const Slice& b) const{
            return comparator_->Compare(a, b);
        }

        // 获取下一entry的位置偏移
        inline uint32_t NextEntryOffset() const {
            return (value_.data() + value_.size() - data_);
        }

        // 获取指定索引重启点的位置偏移
        uint32_t GetRestartPoint(uint32_t index) {
            assert(index < num_restarts_);
            return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
        }

        // 跳转到指定索引的重启点
        void SeekToRestartPoint(uint32_t index) {
            key_.clear();
            // 将当前重启点索引改为index
            restart_index_ = index;
            // 获取重启点的位置偏移
            uint32_t offset = GetRestartPoint(index);
            value_ = Slice(data_ + offset, 0);
        }

        public:
        Iter(const Comparator* comparator, const char* data, uint32_t restarts,
             uint32_t num_restarts)
            :   comparator_(comparator),
                data_(data),
                restarts_(restarts),
                num_restarts_(num_restarts),
                current_(restarts_),
                restart_index_(num_restarts_) {
            
            assert(num_restarts_ > 0);
        }

        bool Valid() const override { return current_ < restarts_; }
        Status status() const override { return status_; }
        Slice key() const override {
            assert(Valid());
            return key_;
        }
        Slice value() const override {
            assert(Valid());
            return value_;
        }

        void Next() override {
            assert(Valid());
            ParseNextKey();
        }
        
        void Prev() override {
            assert(Valid());
            // 获取当前位置偏移之前的一个重启点的索引
            // 
            const uint32_t original = current_;
            while(GetRestartPoint(restart_index_) >= original) {
                if(restart_index_ == 0) {
                    // 找到头了，没有满足条件的重启点
                    current_ = restarts_;
                    restart_index_ = num_restarts_;
                    return ;
                }
                restart_index_-- ;
            }
            SeekToRestartPoint(restart_index_);
            do{
                // 循环，直到找到原始位置偏移前的一个entry
            }while(ParseNextKey() && NextEntryOffset() < original);
        }

        void Seek(const Slice& target) override {
            // 在重启点数组执行二分查找，找到满足key < target key的
            // 最后一个key
            uint32_t left = 0;
            uint32_t right = num_restarts_ - 1;

            int current_key_compare = 0;
            if(Valid()) {
                // 若已经扫描过，使用当前key作为扫描的起始点
                current_key_compare = Compare(key_, target);
                // 判断当前key是在target key的左边还是右边
                if(current_key_compare < 0) {
                    left = restart_index_;
                } else if(current_key_compare > 0) {
                    right = restart_index_;
                } else {
                    return ;
                }
            }

            // 执行二分查找
            while(left < right) {
                // 取中间的重启点索引
                uint32_t mid = (left + right + 1) / 2;
                uint32_t region_offset = GetRestartPoint(mid);
                uint32_t shared, non_shared, value_length;
                // 获取重启点数据，并得到重启点的delta key指针
                const char* key_ptr = 
                            DecodeEntry(data_ + region_offset, data_ + restarts_, &shared, &non_shared, &value_length);
                
                if(key_ptr == nullptr || shared !=0 ) {
                    CorruptionError();
                    return;
                }
                // 获取重启点的key
                Slice mid_key(key_ptr, non_shared);
                if(Compare(mid_key, target) < 0) {
                    left = mid;
                } else {
                    right = mid - 1;
                }
            }

            assert(current_key_compare == 0 || Valid());
            // 找到的重启点是否在当前位置偏移的右边
            bool skip_seek = left == restart_index_ && current_key_compare < 0;
            // 在左边的话要跳过去，在右边的话不用跳，可以直接执行
            // 下面的线性向右查找
            if(!skip_seek) {
                SeekToRestartPoint(left);
            }
            // 执行线性查找
            while(true) {
                // 没有下一个entry了就返回
                if(!ParseNextKey()) {
                    return ;
                }
                // 找到了更应该返回
                if(Compare(key_, target) >= 0) {
                    return ;
                }
            }

        }

        void SeekToFirst() override {
            // 跳到第一个重启点
            SeekToRestartPoint(0);
            // 解析下一entry
            ParseNextKey();
        }
        
        void SeekToLast() override {
            // 跳到最后一个重启点
            SeekToRestartPoint(num_restarts_ - 1);
            // 然后一直往后next就行了
            while(ParseNextKey() && NextEntryOffset() < restarts_) {
                // 一直往后next，直到重启点偏移数组之前
            }
        }

        private:
        void CorruptionError() {
            current_ = restarts_;
            restart_index_ = num_restarts_;
            status_ = Status::Corruption("bad entry in block");
            key_.clear();
            value_.clear();
        }

        // 解析下一个entry
        // 成功返回true，失败返回false
        bool ParseNextKey() {
            // 先获取下一entry的位置偏移
            current_ = NextEntryOffset();
            // 获取指向下一entry数据指针
            const char* p = data_ + current_;
            // restarts_也即重启点位置偏移数组的位置总是在entry的后面
            const char* limit = data_ + restarts_;
            if(p >= limit) {
                // 没有下一entry了
                // 标记无效
                current_ = restarts_;
                restart_index_ = num_restarts_;
                return false;
            }

            // 解码下一entry
            uint32_t shared, non_shared, value_length;
            // 返回的p指向delta key
            p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);
            if(p == nullptr || key_.size()) {
                CorruptionError();
                return false;
            } else {
                key_.resize(shared);
                key_.append(p, non_shared);
                value_ = Slice(p + non_shared, value_length);
                // 更新当前重启点索引
                while(restart_index_ + 1 < num_restarts_ && 
                      GetRestartPoint(restart_index_ + 1) < current_) {
                    // 当前重启点索引还不是距离当前位置偏移最近的重启点索引，
                    // 需要继续往后移动
                    ++restart_index_;
                }
                return true;
            }
        }
    };

    Iterator* Block::NewIterator(const Comparator* comparator) {
        // 重启点数量就占用一个uint32_t来存储
        if(size_ < sizeof(uint32_t)) {
            return NewErrorIterator(Status::Corruption("bad block contents"));
        }

        // 获取重启点数量
        const uint32_t num_restarts = NumRestarts();
        if(num_restarts == 0) {
            // 返回一个空的迭代器
            return NewEmptyIterator();
        } else {
            return new Iter(comparator, data_, restart_offset_, num_restarts);
        }
    }

} // end namespace leveldb