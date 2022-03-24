#include "leveldb/status.h"
#include <cstdio>

namespace leveldb {

    // 拷贝一个state，并将拷贝的副本返回
    const char* Status:: CopyState(const char* state) {
        uint32_t size; // 用来存储从state中读取的msg的长度，占4个字节
        std::memcpy(&size, state, sizeof(size)); // 读取state中存储的msg的长度
        // 根据读取的msg长度分配空间，4个字节长度位 + 1个字节code位 + size个字节消息位
        // 所以共分配 5 + size个字节
        char* result = new char[size + 5]; 
        std::memcpy(result, state, size + 5);

        return result;
    }
    // Status构造函数，根据code和两个msg来构造
    Status::Status(Code code, const Slice& msg, const Slice& msg2) {
        assert(code != kOk);
        const uint32_t len1 = static_cast<uint32_t>(msg.size());
        const uint32_t len2 = static_cast<uint32_t>(msg2.size());
        const uint32_t size = len1 + (len2 ? (2 + len2) : 0); // 计算总的消息位长度，msg2不为空的话多加2个字节来存':'和' '
        // 分配空间
        char* result = new char[5 + size]; // 4个字节长度位 + 1个字节code位 + size个字节的消息位
        std::memcpy(result, &size, sizeof(size)); // 构造长度部分
        result[4] = static_cast<char>(code); // 构造code部分
        std::memcpy(result + 5, msg.data(), len1); // 将msg添加到信息部分
        // 如果msg2不为空的话也添加进去
        if(len2) {
            // 在多加的两个字节位添加上信息
            result[5 + len1] = ':';
            result[6 + len2] = ' ';
            std::memcpy(result + 7 + len1, msg2.data(), len2); // 将msg2添加到消息部分
        }
        state_ = result;
    }
    // ToString 格式 ： code码 + msg，例如 "NotFound: xxxxxxxx"
    std::string Status::ToString() const {
        if(state_ == nullptr) {
            return "OK";
        } else {
            char tmp[30];
            const char* type;
            switch(code()) {
                case kOk:
                    type = "OK";
                    break;
                case kNotFound:
                    type = "NotFound: ";
                    break;
                case kCorruption:
                    type = "Corruption: ";
                    break;
                case kNotSupported:
                    type = "Not implemented: ";
                    break;
                case kInvalidArgument:
                    type = "Invalid argument: ";
                    break;
                case kIOError:
                    type = "IO error: ";
                    break;
                default:
                    std::snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(code()));
                    type = tmp;
                    break;
            }
            std::string result(type);
            uint32_t length;
            std::memcpy(&length, state_, sizeof(length)); // 获取msg的长度
            result.append(state_ + 5, length); // 根据msg的长度读取msg，并拼接
            return result;
        }
    }
} // namespace leveldb