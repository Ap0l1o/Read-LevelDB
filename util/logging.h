//
// Created by Ap0l1o on 2022/3/28.
//

#ifndef LLEVELDB_LOGGING_H
#define LLEVELDB_LOGGING_H

#include <cstdint>
#include <cstdio>
#include <string>

#include "port/port.h"

namespace leveldb {

    class Slice;
    class WritableFile;

    // 将num追加写到*str
    void AppendNumberTo(std::string* str, uint64_t num);

    // 将value追加写到*str;
    // value中的不可打印字符要进行转译；
    void AppendEscapedStringTo(std::string* str, const Slice& value);

    // 将num转存为string形式
    std::string NumberToString(uint64_t num);

    // 将value转存为string形式，不可打印字符要进行转译
    std::string EscapeString(const Slice& value);

    // 将Slice数据in 中的数字部分 转为 uint64_t数据val，转换完成后*in要去掉成功转换完成的那一部分
    bool ConsumeDecimalNumber(Slice* in, uint64_t* val);

} // end namespace leveldb


#endif //LLEVELDB_LOGGING_H
