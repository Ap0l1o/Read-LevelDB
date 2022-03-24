#include "util/hash.h"
#include "util/coding.h"
#include <cstring>

// 不知道这个干嘛用的，后面再看 - - ’
// The FALLTHROUGH_INTENDED macro can be used to annotate implicit fall-through
// between switch labels. The real definition should be provided externally.
// This one is a fallback version for unsupported compilers.
#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED \
  do {                       \
  } while (0)
#endif

namespace leveldb {
    uint32_t Hash(const char* data, size_t n, uint32_t seed) {
        // 与Murmur hash算法类似
        const uint32_t m = 0xc6a4a793;
        const uint32_t r = 24;
        // 获取data的尾指针
        const char* limit = data + n;
        uint32_t h = seed ^ (n * m);
        // 同时取出四个字节
        while(data + 4 <= limit) {
            // 解码4个字节的数据
            uint32_t w = DecodeFixed32(data);
            data += 4;
            h += w;
            h *= m;
            h ^= (h >> 16);
        }

        // 取出剩余的字节
        switch(limit - data) {
            case 3:
                h += static_cast<uint8_t>(data[2]) << 16;
                FALLTHROUGH_INTENDED;
            case 2:
                h += static_cast<uint8_t>(data[1]) << 8;
                FALLTHROUGH_INTENDED;
            case 1:
                h += static_cast<uint8_t>(data[0]);
                h *= m;
                h ^= (h >> r);
                break;
        }
        
        return h;
    }
}