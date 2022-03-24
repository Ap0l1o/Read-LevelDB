#include "db/log_writer.h"
#include <cstdint>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

    namespace log {
        
        // 计算每个record类型的crc32，并将其存储到type_cc
        static void InitTypeCrc(uint32_t* type_cc) {
            for(int i=0; i<=kMaxRecordType; i++) {
                char t = static_cast<char>(i);
                type_cc[i] = crc32c::Value(&t, 1);
            }
        }

        Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
            InitTypeCrc(type_crc_);
        }
        Writer::Writer(WritableFile* dest, uint64_t dest_length) 
            : dest_(dest), block_offset_(dest_length % kBlockSize) {
            InitTypeCrc(type_crc_);
        }

        Writer::~Writer() = default;

        Status Writer::AddRecord(const Slice& slice) {
            // 获取数据指针和长度
            const char* ptr = slice.data();
            size_t left = slice.size();
            // 如果必要的话，要对slice进行分段
            Status s;
            bool begin = true;
            do {
                
                // 获取当前block的插入位置
                const int leftover = kBlockSize - block_offset_;
                assert(leftover >= 0);
                // 若当前block已经连record header都无法存入了
                // 转到一个新的区块
                if(leftover < kHeaderSize) {
                    if(leftover > 0) {
                        // 静态断言
                        // 第一个参数为判断语句，第二个参数为错误提示
                        static_assert(kHeaderSize == 7, "");
                        // 为当前block补位
                        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
                    }
                    // 重置偏移
                    block_offset_ = 0;
                }

                assert(kBlockSize - block_offset_ - kHeaderSize >= 0);
                // 获取block的可用空间
                const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
                // 计算分段长度，空间足够不用分段的话，分段长度就是数据长度，否则分段长度为当前可用长度
                const size_t fragment_length = (left < avail) ? left : avail;

                RecordType type;
                // left == fragment表示没有分段，已经结束
                const bool end = (left == fragment_length);
                if(begin && end) {
                    type = kFullType;
                } else if(begin) {
                    type = kFirstType;
                } else if(end) {
                    type = kLastType;
                } else {
                    type = kMiddleType;
                }
                
                // 写入数据
                s = EmitPhysicalRecord(type, ptr, fragment_length);
                // 修改相关标记
                // 指针后移
                ptr += fragment_length;
                // 修改剩余数据长度
                left -= fragment_length;
                // 已经写入过一次了，不是首次写入，begin改为false
                begin = false;
            } while(s.ok() && left > 0);

            return s;
        }

        Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t length) {
            // 因为header中的length只分配了两个字节，
            // 所以不能超过两个字节能表示的大小
            assert(length <= 0xffff);
            assert(block_offset_ + kHeaderSize + length <= kBlockSize);

            // 构建header，其中：
            // checksum (4 bytes), length (2 bytes), type (1 byte)
            char buf[kHeaderSize];
            // 存入length
            buf[4] = static_cast<char>(length & 0xff);
            buf[5] = static_cast<char>(length >> 8);
            // 存入record type
            buf[6] = static_cast<char>(t);
            // 计算并存入crc检验和
            uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
            crc = crc32c::Mask(crc);
            EncodeFixed32(buf, crc);

            // 写入header
            Status s = dest_->Append(Slice(buf, kHeaderSize()));
            // 成功写入header后，继续写入数据
            if(s.ok()) {
                s = dest_->Append(Slice(ptr, length));
                if(s.ok()) {
                    s = dest_->Flush();
                }
            }
            // 修改偏移
            block_offset_ += kHeaderSize + length;
            return s;
        }

    } // end namespace log

} // end namespace leveldb