#ifndef COMPARATOR_H_
#define COMPARATOR_H_

#include <string>
#include "leveldb/export.h"
namespace leveldb {
    class Slice;
    // A Comparator object provides a total order across slices that are
    // used as keys in an sstable or a database.  A Comparator implementation
    // must be thread-safe since leveldb may invoke its methods concurrently
    // from multiple threads.
    // 定义一个Comparator接口，用来根据key排序
    class LEVELDB_EXPORT Comparator {
    public:
        virtual ~Comparator();

        // Three-way comparison.  Returns value:
        //   < 0 iff "a" < "b",
        //   == 0 iff "a" == "b",
        //   > 0 iff "a" > "b"
        virtual int Compare(const Slice& a, const Slice& b) const = 0;

        // The name of the comparator.  Used to check for comparator
        // mismatches (i.e., a DB created with one comparator is
        // accessed using a different comparator.
        //
        // The client of this package should switch to a new name whenever
        // the comparator implementation changes in a way that will cause
        // the relative ordering of any two keys to change.
        //
        // Names starting with "leveldb." are reserved and should not be used
        // by any clients of this package.
        // Comparator有其name，用于区分
        virtual const char* Name() const = 0;

        // Advanced functions: these are used to reduce the space requirements
        // for internal data structures like index blocks.
        // 这两个函数用于减少像index blocks这样的内部数据结构占用的空间
        // If *start < limit, changes *start to a short string in [start,limit).
        // 如果*start < limit, 就在[start, limit)中找到一个短字符串赋值给*start返回
        // Simple comparator implementations may return with *start unchanged,
        // 简单的comparator实现可能不改变*start
        // i.e., an implementation of this method that does nothing is correct.
        virtual void FindShortestSeparator(std::string* start, const Slice& limit) const = 0;
        // Changes *key to a short string >= *key.
        // 找到一个 >= *key 的短字符串赋值给*key返回
        // Simple comparator implementations may return with *key unchanged,
        // i.e., an implementation of this method that does nothing is correct.
        virtual void FindShortSuccessor(std::string* key) const = 0;
    };
    // Return a builtin comparator that uses lexicographic byte-wise
    // ordering.  The result remains the property of this module and
    // must not be deleted.
    LEVELDB_EXPORT const Comparator* BytewiseComparator();
} // namespace leveldb


#endif