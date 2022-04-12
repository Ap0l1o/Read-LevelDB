
#ifndef LOG_WRITER_H_
#define LOG_WRITER_H_

#include <cstdint>
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "db/log_format.h"

namespace leveldb {

    class WritableFile;

    namespace log {

        class Writer {

            public:
            // 创建一个writer，将数据写到 *dest
            // *dest必须初始化为空
            // 在此writer使用期间，此*dest必须有效
            explicit Writer(WritableFile* dest);
            // 创建一个writer，将数据写到 *dest
            // *dest 的长度必须初始化为 dest_length
            // 在此writer使用期间，此*dest必须有效
            Writer(WritableFile* dest, uint64_t dest_length);

            Writer(const Writer&) = delete;
            Writer& operator=(const Writer&) = delete;

            ~Writer();

            Status AddRecord(const Slice& slice);

            private:
            //
            Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

            // 顺序写的日志文件对象
            WritableFile* dest_;
            // block 中当前的偏移位置
            int block_offset_;  

            // 存放为各个类型的RecordType所计算的crc32，以减少开销
            uint32_t type_crc_[kMaxRecordType + 1];
        }; // end class Writer

    } // end namespace log

} // end namespace leveldb


#endif // endif LOG_WRITER_H_