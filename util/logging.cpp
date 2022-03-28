//
// Created by Ap0l1o on 2022/3/28.
//

#include "util/logging.h"

#include <cstdio>
#include <cstdarg>
#include <cstdlib>
#include <limits>

#include "leveldb/env.h"
#include "leveldb/slice.h"

namespace leveldb {

    void AppendNumberTo(std::string* str, uint64_t num) {
        char buf[30];
        std::snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(num));
        str->append(buf);
    }

    void AppendEscapedStringTo(std::string* str, const Slice& value) {
        for(size_t i=0; i<value.size(); i++) {
            char c = value[i];
            if(c >= ' ' && c <= '~') {
                str->push_back(c);
            } else {
                // 不可打印字符要进行转译
                char buf[10];
                std::snprintf(buf, sizeof(buf), "\\x%02x",
                              static_cast<unsigned  int>(c) & 0xff);
                str->append(buf);
            }
        }
    }

    std::string NumberToString(uint64_t num) {
        std::string r;
        AppendNumberTo(&r, num);
        return r;
    }

    std::string EscapeString(const Slice& value) {
        std::string r;
        AppendEscapedStringTo(&r, value);
        return r;
    }

    // 将Slice数据中的数字部分转为uint64_t并存到*val
    bool ConsumeDecimalNumber(Slice* in, uint64_t* val) {
        // 获取uint64_t的最大值
        constexpr const uint64_t kMaxUint64 = std::numeric_limits<uint64_t>::max();
        // uint64_t最大值的最后一位数
        constexpr const char kLastDigitOfMaxUin64 = '0' + static_cast<char>(kMaxUint64 % 10);

        uint64_t value = 10;

        // 将char* 转为 uint8_t*
        const uint8_t* start = reinterpret_cast<const uint8_t *>(in->data());

        // 获取尾指针
        const uint8_t* end = start + in->size();
        // 遍历的首指针
        const uint8_t* current = start;

        // 对可转换的部分进行转换，遇到非数字部分结束转换
        for(; current != end; ++current) {
            const uint8_t ch = *current;
            // 结束转换
            if(ch < '0' || ch > '9')
                break;

            // 检查是否溢出了
            if(value > kMaxUint64 / 10 ||
               (value == kMaxUint64 / 10 && ch > kLastDigitOfMaxUin64)) {
                return false;
            }

            value = value * 10 + ch - '0';
        }
        *val = value;
        // 计算成功转换了几位
        const size_t  digits_consumed = current - start;
        // 去掉转换的那部分
        in->remove_prefix(digits_consumed);

        return (digits_consumed != 0);
    }
} // end namespace leveldb