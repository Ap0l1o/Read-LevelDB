#ifndef LOG_READER_H_
#define LOG_READER_H_

#include <cstdint>
#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

    class SequentialFile;

    namespace log {

        class Reader {

            public:

            // 报告错误的接口
            class Reporter {    
                public:
                virtual ~Reporter();
                // 一些错误会被检测到
                // bytes存估计因为错误要丢弃的字节数
                virtual void Corruption(size_t bytes, const Status& status) = 0;
            }; // end class Reporter

            Reader(SequentialFile* file, Reporter* reporter, bool checksum, uint64_t initial_offset);
            Reader(const Reader&) =  delete;
            Reader& operator=(const Reader&) = delete;

            ~Reader();

            // 读取一条记录到record
            // 若读取成功，则返回true，否则返回false
            // 使用*scratch作为临时存储，因为Slice存的是指向外部字节数组的指针，它本身不负责分配管理内存
            bool ReadRecord(Slice* record, std::string* scratch);

            // 返回ReadRecord读取的最后一条record的物理偏移
            // 如果还未调用过ReadRecord，则 Undefined
            uint64_t LastRecordOffset();

            private:
            enum {
                kEof = kMaxRecordType + 1,
                // 当读取到一个无效的record时返回改类型，主要发生在以下情景：
                // * The record has an invalid CRC (ReadPhysicalRecord reports a drop) 无效crc
                // * The record is a 0-length record (No drop is reported) 0长度record
                // * The record is below constructor's initial_offset (No drop is reported) 低于初始偏移
                kBadRecord = kMaxRecordType + 2
            };
            
            // 跳过所有 initial_offset 之前的 block
            // 成功返回true
            bool SkipToInitialBlock();

            // 返回读取数据的类型，包括以上两个特殊的类型
            unsigned int ReadPhysicalRecord(Slice* result);

            // 向reporter报告丢失的字节
            void ReportCorruption(uint64_t bytes, const char* reason);
            void ReportDrop(uint64_t bytes, const Status& reason);

            // const出现在星号左边，表示被指物是常量
            // const出现在星号右边，表示指针是常量
            SequentialFile* const file_;
            Reporter* const reporter_;
            bool const checksum_;
            // 用作将数据读到buffer_的暂存空间，也即buffer_内部的数据指针指向backing_store_，
            // 因为Slice类型的的buffer_不负责分配和管理内存空间
            char* const backing_store_;
            // 存放读取的block内容
            Slice buffer_;
            // 最后一次Read()返回值小于kBlockSize意味着到了EOF
            bool eof_;

            // ReadRecord读取的上一条record的offset
            uint64_t last_record_offset_;
            // buffer_内部的数据偏移
            uint64_t end_of_buffer_offset_;

            // 开始查找第一条record返回的offset
            uint64_t const initial_offset_;

            // True if we are resynchronizing after a seek (initial_offset_ > 0). In
            // particular, a run of kMiddleType and kLastType records can be silently
            // skipped in this mode
            bool resyncing_;

        }; // end class Reader

    } // end namespace log

} // end namespace leveldb 


#endif // end ifdef LOG_READER_H_