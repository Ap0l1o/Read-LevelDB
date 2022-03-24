
#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "leveldb/env.h"

namespace leveldb {
    
    // 提取数据，有效数据的长度在其固定长度的前缀中（前4个字节） length = varint32
    // +5 是因为第五个已经是上限limit了必须要小于上限
    static Slice GetLengthPrefixedSlice(const char* data) {
        uint32_t len;
        const char* p = data;
        // 提取长度信息，并将指针向前移动
        p = GetVarint32Ptr(p, p+5, &len);
        // 根据获取的数据长度信息提取数据
        return Slice(p, len);
    }

    MemTable::MemTable(const InternalKeyComparator& comparator)
        : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}
    
    MemTable::~MemTable() { assert(refs_ == 0); }

    size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

    int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr) const {
        // internal key编码后将其长度存在其前缀中
        Slice a = GetLengthPrefixedSlice(aptr);
        Slice b = GetLengthPrefixedSlice(bptr);
        return comparator.Compare(a, b);
    }

    // 将target编码为internal key，并将其暂存在*scratch中，
    // 返回指向scratch空间的指针
    static const char* EncodeKey(std::string* scratch, const Slice& target) {
        scratch->clear();
        // 编码target的长度，并将其存到scratch
        PutVarint32(scratch, target.size());
        // 将数据存到scratch
        scratch->append(target.data(), target.size());
        // 返回指向数据空间的指针
        return scratch->data();
    }

    class MemTableIterator : public Iterator {

        public:
        explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}
        MemTableIterator(const MemTableIterator&) = delete;
        MemTableIterator& operator=(const MemTableIterator&) = delete;

        ~MemTableIterator() override = default;

        bool Valid() const override { return iter_.Valid(); }
        void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
        void SeekToFirst() override { return iter_.SeekToFirst(); }
        void SeekToLast() override { return iter_.SeekToLast(); }
        void Next() override { iter_.Next(); }
        void Prev() override { iter_.Prev(); }
        Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
        Slice value() const override {
            Slice key_slice = GetLengthPrefixedSlice(iter_.key());
            return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
        }

        Status status() const override { return Status::OK(); }

        private:
        MemTable::Table::Iterator iter_;
        std::string tmp_;
    };

    Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

    

    void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key, const Slice& value) {
        // Format of an entry is concatenation of:
        //  key_size     : varint32 of internal_key.size()
        //  key bytes    : char[internal_key.size()]
        //  value_size   : varint32 of value.size()
        //  value bytes  : char[value.size()]
        // 最后插入table的顺序为：
        // key size + key + tag(seq + type) + value size + value
        size_t key_size = key.size();
        size_t val_size = value.size();
        // seq 7 + type 1 = 8
        size_t internal_key_size = key_size + 8;
        // 计算编码后的字节长度: 数据的实际大小+其长度所占用的大小
        const size_t encoded_len = VarintLength(internal_key_size) +
                                   internal_key_size + VarintLength(val_size) + val_size;
        
        // 从内存池分配空间
        char* buf = arena_.Allocate(encoded_len);
        //将internal key的长度存入buf，返回的p是当前指针的位置
        char* p = EncodeVarint32(buf, internal_key_size);
        // 继续将internal key的数据部分存入
        std::memcpy(p, key.data(), key_size);
        // 指针继续后移
        p += key_size;
        // 对seq和type编码，并将其存入
        EncodeFixed64(p, (s <<  8) | type);
        // 指针继续后移，seq + type = 8 byte
        p += 8;
        // 将value的长度存入
        p = EncodeVarint32(p, val_size);
        // 将value的数据存入
        std::memcpy(p, value.data(), val_size);
        assert(p + val_size == buf + encoded_len);
        table_.Insert(buf);
    }

    // 根据lookup key查询，将查找到的值存入*value，状态码存入*s
    bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
        // 获取memtable key : key length + user key + tag
        Slice memkey = key.memtable_key();
        // 创建当前跳表的迭代器
        Table::Iterator iter(&table_);
        // 查找
        iter.Seek(memkey.data());

        // entry format is:
        //    klength  varint32
        //    userkey  char[klength]
        //    tag      uint64
        //    vlength  varint32
        //    value    char[vlength]
        if(iter.Valid()) {
            const char* entry = iter.key();
            uint32_t key_length;
            // 提取key length
            const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
            // 判断找到的key是不是我们要找的key，即判断key是否相等
            if(comparator_.comparator.user_comparator()->Compare(
                Slice(key_ptr, key_length-8), key.user_key()) == 0 ) {
                
                // 若相等，即compare结果为0，则找到正确的key
                // 提取tag
                const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
                // 根据value的类型处理
                switch(static_cast<ValueType>(tag & 0xff)) {
                    case kTypeValue: {
                        // 提取value
                        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
                        value->assign(v.data(), v.size());
                        return true;
                    }
                    case kTypeDeletion: {
                        *s = Status::NotFound(Slice());
                        return true;
                    }
                }
            }
            return false;
        }
    }
} // end namespace leveldb 