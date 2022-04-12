
// log format信息是由 reader 和 writer共享的
// 具体的信息可以看/doc/log_format.md

#ifndef LOG_FORMAT_H_
#define LOG_FORMAT_H_

namespace leveldb{

    namespace log{

        enum RecordType{
            // 为预分配文件所保留
            kZeroType = 0,
            // record完整的保存在一个record中
            kFullType = 1,
            // 分段存储
            kFirstType = 2,
            kMiddleType = 3,
            kLastType = 4
        }; // end enum RecordType

        // record类型的最大值
        static const int kMaxRecordType = kLastType;
        // 1024B * 32 = 32768, block大小为32KB；
        // leveldb对于一个log文件，会把它切割成以32KB为单位的物理BLock，每次读取以
        // 一个Block作为基本读取单位。一个record可能会占用几个Block。
        static const int kBlockSize = 32768;
        // Header is checksum (4 bytes), length (2 bytes), type (1 byte).
        static const int kHeaderSize = 4 + 2 + 1;
        
    } // end namespace log

} // end namespace leveldb

#endif // LOG_FORMAT_H_