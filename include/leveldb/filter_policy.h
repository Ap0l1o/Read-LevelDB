#ifndef FILTER_POLICY_H_
#define FILTER_POLICY_H_

#include <string>
#include "leveldb/export.h"

namespace leveldb {
    class Slice;

    class LEVELDB_EXPORT FilterPolicy {
    public:
        virtual ~FilterPolicy();
        // 返回FilterPolicy的名字，用于唯一标识一个FilterPolicy
        virtual const char* Name() const = 0;
        // keys[0, n-1]保存了使用comparator所构造的有序keys
        // 且对其附加一个Filter， 将其汇总到*dst中
        virtual void CreateFilter(const Slice* keys, int n,
                                  std::string* dst) const = 0;
        // 当CreateFilter传入keys所创建的filter包含key时，返回true。
        // 当不包含时返回true或false，返回false的概率应该更高
        virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;

    };

    // const 出现在星号左边，表示被指物是常量；const出现在星号右边，表示指针自身是常量；
    // 使用一个bloom filter 返回一个新的 filter policy，使用bits_per_key来制定每个key所占用的bits。
    // 当bits_per_key为10时，误报率大概为1%，这是一个比较理想的情况
    LEVELDB_EXPORT const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);

} // end namespace leveldb


#endif // FILTER_POLICY_H_