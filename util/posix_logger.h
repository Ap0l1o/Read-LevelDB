//
// Created by Ap0l1o on 2022/3/30.
//

#ifndef LLEVELDB_POSIX_LOGGER_H
#define LLEVELDB_POSIX_LOGGER_H

#include <sys/time.h>

#include <cassert>
#include <cstdio>
#include <ctime>
#include <sstream>
#include <thread>

#include "leveldb/env.h"

namespace leveldb {
    // 写日志类
    class PosixLogger final : public Logger {
    public:
        // 创建一个写到指定文件的logger
        // PosixLogger实例控制文件句柄
        explicit PosixLogger(std::FILE* fp) : fp_(fp) {
            assert(fp != nullptr);
        }

        ~PosixLogger() override {
            std::fclose(fp_);
        }

        void Logv(const char* format, std::va_list arguments) override {
            // 获取当前的时间
            struct ::timeval now_timeval;
            ::gettimeofday(&now_timeval, nullptr);
            const std::time_t now_seconds = now_timeval.tv_sec;
            struct std::tm now_components;
            // 转换时间格式
            ::localtime_r(&now_seconds, &now_components);

            // 记录线程ID
            constexpr const int kMaxThreadIdSize = 32;
            std::ostringstream thread_stream;
            thread_stream << std::this_thread::get_id;
            std::string thread_id = thread_stream.str();
            if(thread_id.size() > kMaxThreadIdSize) {
                thread_id.resize(kMaxThreadIdSize);
            }

            // 先尝试输出到栈buffer，如果失败了则再尝试动态分配的buffer（也即堆buffer）
            constexpr const int kStackBufferSize = 512;
            char stack_buffer[kStackBufferSize];
            // 检查是否分配成功
            static_assert(sizeof(stack_buffer) == static_cast<size_t>(kStackBufferSize),
                          "sizeof(char) is expected to be 1 in C++");

            // dynamic_buffer_size在第一次进入循环后进行计算
            int dynamic_buffer_size = 0;
            for(int iteration = 0; iteration < 2; ++iteration) {
                // 第一次进入循环iteration为0，此时尝试栈buffer，
                // 若成功输出到栈buffer则通过break直接结束循环
                const int buffer_size =
                        (iteration == 0) ? kStackBufferSize : dynamic_buffer_size;

                // 第一层进入循环时，iteration为0，此时不进行动态分配堆buffer，还是使用栈buffer。
                // 若没能成功输出到栈buffer，第二次进入循环时iteration不为0，dynamic_buffer_size已成功得到
                // 计算，此时进行动态分配堆buffer。
                char* const buffer =
                        (iteration == 0) ? stack_buffer : new char[dynamic_buffer_size];

                // 将header信息输出到buffer, 并获取返回的header长度
                int buffer_offset = std::snprintf(buffer, buffer_size,
                                                  "%04d/%02d/%02d-%02d:%02d:%02d:%02d.%06d %s",
                                                  now_components.tm_year + 1900, now_components.tm_mon + 1,
                                                  now_components.tm_mday, now_components.tm_hour, now_components.tm_min,
                                                  now_components.tm_sec, static_cast<int>(now_timeval.tv_usec),
                                                  thread_id.c_str());

                // header最多包括28个字符（10字符的date + 15字符的time + 3字符的分隔符）再加上thread ID，
                // 更适合使用static buffer
                assert(buffer_offset <= 28 + kMaxThreadIdSize);
                static_assert(28 + kMaxThreadIdSize < kStackBufferSize,
                              "stack-allocated buffer may not fit the message header");
                assert(buffer_offset < buffer_size);

                // 将message输出到buffer
                std::va_list arguments_copy;
                va_copy(arguments_copy, arguments);
                buffer_offset +=
                        std::vsnprintf(buffer + buffer_offset, buffer_size - buffer_offset,
                                       format, arguments_copy);
                va_end(arguments_copy);

                // 栈buffer不够用
                if(buffer_size >= buffer_size - 1) {
                    if(iteration == 0) {
                        // 计算buffer需要的空间，以动态分配堆buffer
                        dynamic_buffer_size = buffer_offset + 2;
                        // 进入第二次循环，动态分配堆buffer
                        continue;
                    }

                    assert(false);
                    buffer_offset = buffer_size - 1;
                }

                if(buffer[buffer_offset - 1] != '\n') {
                    buffer[buffer_offset] = '\n';
                    ++buffer_offset;
                }

                assert(buffer_offset <= buffer_size);
                std::fwrite(buffer, 1, buffer_offset, fp_);
                std::fflush(fp_);

                // iteration不为0，说明栈buffer不够用，用的是堆buffer，
                // 需要手动释放堆buffer
                if(iteration != 0) {
                    delete[] buffer;
                }
                break;
            }
        }

    private:
        std::FILE* const fp_;
    };

} // end namespace leveldb


#endif //LLEVELDB_POSIX_LOGGER_H
