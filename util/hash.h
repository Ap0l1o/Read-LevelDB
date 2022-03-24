#ifndef HASH_H_
#define HASH_H_

#include <cstddef>
#include <cstdint>

namespace leveldb {

    // 根据seed对data计算hash值(4字节)
    uint32_t Hash(const char* data, size_t n, uint32_t seed);

} // end namespace leveldb

#endif // end HASH_H_