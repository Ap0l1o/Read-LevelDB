
#ifndef MEMTABLE_H_
#define MEMTABLE_H_

#include <string>
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "leveldb/iterator.h"
#include "leveldb/db.h"
#include "util/arena.h"

namespace leveldb {

    class InternalKeyComparator;
    class MemTableIterator;

    class MemTable {
        public:
        // MemTable会通过引用计数的，初始化时引用计数为0，每次被调用必须先调用Ref()来增加一次引用，
        // 引用计数不为0则不能被删除。
        explicit MemTable(const InternalKeyComparator& comparator);

        MemTable(const MemTable&) = delete;
        MemTable& operator=(const MemTable&) = delete;

        // 增加引用计数
        void Ref() { ++refs_; }
        // 减少引用计数，当无引用时删除
        void Unref() {
            --refs_;
            assert(refs_ >= 0);
            if(refs_ <= 0) {
                delete this;
            }
        }

        // 返回使用此数据结构的数据预计所占用的字节数
        size_t ApproximateMemoryUsage();

        // 返回遍历Memtable的迭代器
        //
        // The caller must ensure that the underlying MemTable remains live
        // while the returned iterator is live.  The keys returned by this
        // iterator are internal keys encoded by AppendInternalKey in the
        // db/format.{h,cc} module.
        Iterator* NewIterator();

        // 向Memtable中添加数据项，按照特定的seq和type将key映射到value
        // 需要注意的是，当type==kTypeDeletion时，value是空值
        void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);
        
        // 如果Memtable包含key的value，则将value存到*value并返回true
        // 若Memtable中存的是有删除标记的key，则在*status中保存一个NotFound()错误，并返回true
        // 否则返回false
        bool Get(const LookupKey& key, std::string* value, Status* s);

        private:
        friend class MemTableIterator;
        friend class MemTableBackwardIterator;

        struct KeyComparator {
            const InternalKeyComparator comparator;
            explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
            int operator()(const char* a, const char* b) const;
        };

        typedef SkipList<const char*, KeyComparator> Table;
        ~MemTable();

        KeyComparator comparator_;
        int refs_;
        Arena arena_;
        Table table_;
    };

} // end namespace leveldb 


#endif // end MEMTABLE_H_