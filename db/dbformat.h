#ifndef DBFORMAT_H_
#define DBFORMAT_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "leveldb/comparator.h"
//#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
//#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {
    // 常量组，我们可能希望通过options来设置其中的部分参数
    namespace config {
        // Level的最大数量
        static const int kNumLevels = 7;
        // Level-0 文件数量阈值，超过此值将执行Compaction
        static const int kL0_CompactionTrigger = 4;
        // 对 Level-0 文件的数量进行软限制，此时，限制写入速度
        static const int kL0_SlowdownWriteTriger = 8;
        // Level-0 的最大文件数量，此时将停止写入
        static const int kL0_StopWritesTrigger = 12;
        // Compaction执行的结果最大将被push到哪个level
        // Maximum level to which a new compacted memtable is pushed if it
        // does not create overlap.  We try to push to level 2 to avoid the
        // relatively expensive level 0=>1 compactions and to avoid some
        // expensive manifest file operations.  We do not push all the way to
        // the largest level since that can generate a lot of wasted disk
        // space if the same key space is being repeatedly overwritten.
        static const int kMaxMemCompactLevel = 2;
        // Approximate gap in bytes between samples of data read during iteration.
        static const int kReadBytesPeriod = 1048576;
    } // end namespace config

    class InternalKey;
    // InternalKey的最后一个组件，value的类型，是添加新数据还是删除数据
    // 需要注意的是该枚举类型不要更改，这个要写入磁盘的
    enum ValueType { kTypeDeletion = 0x0, kTypeValue = 0x1 };

    // 用于执行seek操作
    static const ValueType kValueTypeForSeek = kTypeValue;
    // 操作序号
    typedef uint64_t SequenceNumber;

    // 设置序号最大值
    // SequenceNumber是64位，这里将其最后一个字节留空来存ValueType，
    // 因此可以将SequenceNumber 和 ValueType存到一个8个字节的uint64_t
    static const SequenceNumber kMaxSequenceNumber = ( (0x1ull << 56) - 1 );

    // 用于存储InternalKey分解后的数据，可以分解为三部分:
    // 用户key，序号，类型
    struct ParsedInternalKey {
        Slice user_key;
        SequenceNumber sequence;
        ValueType type;
        ParsedInternalKey() {}
        ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t) 
            : user_key(u), sequence(seq), type(t) {}
        
        std::string DebugString() const;
    };

    // 返回key编码为InternalKey后的长度，因为序号+类型占用8个字节，所以要加8
    inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
        return key.user_key.size() + 8;
    }

    // 将序列化后的key添加到 *result
    void AppendInternalKey(std::string* result, const ParsedInternalKey& key);
    
    // 尝试解析一个InternalKey，若
    // 成功：将解析结果存到 *result，返回true
    // 失败：返回false，将*result置为未定义状态
    bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result);

    // 从InternalKey中提取user_key
    inline Slice ExtractUserKey(const Slice& internal_key) {
        // 序号和类型就占用8个字节，长度小于8个字节不合理
        assert(internal_key.size() > 8);
        return Slice(internal_key.data(), internal_key.size() - 8);
    }

    // 为InternalKey设计的Comparator，使用指定的Comparator
    class InternalKeyComparator : public Comparator {
        private:
        const Comparator* user_comparator_;

        public:
        explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) {}
        const char* Name() const override;
        int Compare(const Slice& a, const Slice& b) const override;
        void FindShortestSeparator(std::string* start, const Slice& limit) const override;
        void FindShortSuccessor(std::string* key) const override;
        const  Comparator* user_comparator() const { return user_comparator_; }
        int Compare(const InternalKey& a, const InternalKey& b) const;
    };

    // 从InternalKey转为user key的过滤策略包装器
    // 就是先将InternalKey转为user key 再使用过滤器
    class InternalFilterPolicy : public FilterPolicy {
        private:
        const FilterPolicy* const user_policy_;

        public:
        explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) {}
        const char* Name() const override;
        void CreateFilter(const Slice* keys, int n, std::string* dst) const override;
        bool KeyMayMatch(const Slice& key, const Slice& filter) const override;
    };

    // InternalKey用于封装user key，以按照相应的规则进行比较
    class InternalKey {
        private:
        std::string rep_;

        public:
        // 默认构造函数，rep_置空，以暗示其不可用
        InternalKey() {}
        InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
            // 先利用ParsedInternalKey各部分组成ParsedInternalKey，再利用
            // AppendInternalKey函数将其序列化并添加到rep_
            AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
        }

        bool DecodeFrom(const Slice& s) {
            rep_.assign(s.data(), s.size());
            return !rep_.empty();
        }
        
        // 获取编码后的数据
        Slice Encode() const {
            assert(!rep_.empty());
            return rep_;
        }

        // 返回user_key
        Slice user_key() const { return ExtractUserKey(rep_); }

        // 根据一个ParsedInternalKey重设InternalKey的值
        void SetFrom(const ParsedInternalKey& p) {
            rep_.clear();
            AppendInternalKey(&rep_, p);
        }

        // 清空
        void Clear() { rep_.clear(); }

        std::string DebugString() const;
    };

    inline int InternalKeyComparator::Compare(const InternalKey& a, const InternalKey& b) const {
        // 调用Compare(Slice& a, Slice& b);
        return Compare(a.Encode(), b.Encode());
    }

    inline bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result) {
        const size_t n = internal_key.size();
        // 序号+类型 = 8 字节，小于8字节不合理
        if(n < 8) return false;
        // 最后8个字节为7序号+1类型
        uint64_t num  = DecodeFixed64(internal_key.data() + n - 8);
        // 获取类型
        uint8_t c = num & 0xff;
        result->sequence = num >> 8;
        result->type = static_cast<ValueType>(c);
        result->user_key = Slice(internal_key.data(), n-8);
        return (c <= static_cast<uint8_t>(kTypeValue));
    }

    // 工具类，用于执行Get()
    class LookupKey {
        public:
        LookupKey(const Slice& user_key, SequenceNumber sequence);

        LookupKey(const LookupKey&) = delete;
        LookupKey& operator=(const LookupKey&) = delete;

        ~LookupKey();

        // 返回一个适用于在Memtable中查找的key
        Slice memtable_key() const { return Slice(start_, end_ - start_); }
        // 返回一个适用于internal iterator的 internal key
        Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }
        // 返回user key
        Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

        private:
        // We construct a char array of the form:
        //    klength  varint32               <-- start_  指向长度 = user key 的长度 + tag的长度
        //    userkey  char[klength]          <-- kstart_ 指向数据
        //    tag      uint64                 <-- seq + type
        //                                    <-- end_  指向结尾
        // 由此可以推断 LookupKey的格式为：length + user key + seq + type
        // The array is a suitable MemTable key.
        // The suffix starting with "userkey" can be used as an InternalKey.
        const char* start_;
        const char* kstart_;
        const char* end_;
        char space_[200];
    };

    inline LookupKey::~LookupKey() {
        if( start_ != space_ )
            delete[] start_; 
    } 

} // end namespace leveldb




#endif // DBFORMAT_H_