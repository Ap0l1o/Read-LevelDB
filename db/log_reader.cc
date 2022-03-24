
#include "db/log_reader.h"
#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
    namespace log {

        Reader::Reporter::~Reporter() = default;

        Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
                       uint64_t initial_offset)
            :   file_(file),
                reporter_(reporter),
                checksum_(checksum),
                backing_store_(new char[kBlockSize]),
                buffer_(),
                eof_(false),
                last_record_offset_(0),
                end_of_buffer_offset_(0),
                initial_offset_(initial_offset),
                resyncing_(initial_offset > 0) {}
        

        Reader::~Reader() { delete[] backing_store_; }

        bool Reader::SkipToInitialBlock() {
            // 跳过所有 initial_offset 之前的 block
            // 此函数用于调整read offset
            
            // 计算在block内的偏移位置
            const size_t offset_in_block = initial_offset_ % kBlockSize;
            // 规整到开始读取block 的起始位置
            uint64_t block_start_location = initial_offset_ - offset_in_block;

            // 若偏移在最后6字节内，则必不是完整记录，跳到下一个block
            if(offset_in_block > kBlockSize - 6) {
                block_start_location += kBlockSize;
            }
            
            end_of_buffer_offset_ = block_start_location;

            // 跳转到包含初始record的第一个block的开始
            if(block_start_location > 0) {
                Status skip_status = file_->Skip(block_start_location);
                if(!skip_status.ok()) {
                    ReportDrop(block_start_location, skip_status);
                    return false;
                }
            }

            return true;
        } 


        bool Reader::ReadRecord(Slice* record, std::string* scratch) {
            // 当前偏移小于指定的偏移，则需要跳转
            if(last_record_offset_ < initial_offset_) {
                if(!SkipToInitialBlock()) {
                    return false;
                }
            }

            scratch->clear();
            record->clear();

            // 当前是否在fragment内，也就是遇到了fist record
            bool in_fragmented_record = false;
            // 正在读取的逻辑record的偏移
            uint64_t prospective_record_offset = 0;

            Slice fragment;
            while(true) {
                // 读取数据到fragment，并接受返回的数据类型
                const unsigned int record_type = ReadPhysicalRecord(&fragment);
                // 计算当前正在读取的record的偏移值
                uint64_t physical_record_offset = 
                    end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

                if(resyncing_) {
                    if(record_type == kMiddleType) {
                        continue;;
                    } else if(record_type == kLastType) {
                        resyncing_ = false;
                        continue;
                    } else {
                        resyncing_ = false;
                    }
                }
                
                switch(record_type) {
                    case kFullType:
                        if(in_fragmented_record) {
                            // 早期版本结尾是空record，没有eof
                            if(!scratch->empty()) {
                                ReportCorruption(scratch->size(), "partial record without end(1)");
                            }
                        }
                        prospective_record_offset = physical_record_offset;
                        scratch->clear();
                        *record = fragment;
                        last_record_offset_ = prospective_record_offset;
                        return true;

                    case kFirstType:
                        if(in_fragmented_record) {
                            if(!scratch->empty()) {
                                ReportCorruption(scratch->size(), "partial record without end(2)");
                            }
                        }
                        prospective_record_offset = physical_record_offset;
                        // 存到scratch
                        scratch->assign(fragment.data(), fragment.size());
                        in_fragmented_record = true;
                        break;
                    
                    case kMiddleType:
                        // 读第一个record时，会标记in_fragmented_record为true
                        // 为false表明丢失第一个record
                        if(!in_fragmented_record) {
                            ReportCorruption(fragment.size(), "missing start of fragmented record(1)");
                        } else {
                            // 追加数据到scratch
                            scratch->append(fragment.data(), fragment.size());
                        }
                        break;
                    
                    case kLastType:
                        if(!in_fragmented_record) {
                            ReportCorruption(fragment.size(), "missing start of fragmented record(2)");
                        } else {
                            // 追加数据到scratch
                            scratch->append(fragment.data(), fragment.size());
                            // 让record指向scratch
                            *record = Slice(*scratch);
                            last_record_offset_ = physical_record_offset;
                            return true;
                        }
                        break;

                    case kEof:
                        if(in_fragmented_record) {
                            scratch->clear();
                        }
                        return false;
                    
                    case kBadRecord:
                        if(in_fragmented_record) {
                            ReportCorruption(scratch->size(), "error in middle of record");
                            in_fragmented_record = false;
                            scratch->clear();
                        }
                        break;

                    default: {
                        char buf[40];
                        std::snprintf(buf, sizeof(buf), "unknow reocrd type %u", record_type);
                        ReportCorruption(
                            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
                            buf
                        );
                        in_fragmented_record = false;
                        scratch->clear();
                        break;
                    }

                }
            }

            return false;
        }

        uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

        void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
            ReportDrop(bytes, Status::Corruption(reason));
        }

        void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
            if(reporter_ != nullptr && 
               end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
                reporter_->Corruption(static_cast<size_t>(bytes), reason);
            }
        }

        unsigned int Reader::ReadPhysicalRecord(Slice* result) {
            while(true) {
                // buffer_内的数据已经小于kHeaderSize
                // 
                if(buffer_.size() < kHeaderSize) {
                    if(!eof_) {
                        buffer_.clear();
                        // 读取一个block数据到buffer_, 使用backing_store_做暂存空间
                        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
                        end_of_buffer_offset_ += buffer_.size();
                        if(!status.ok()) {
                            buffer_.clear();
                            ReportDrop(kBlockSize, status);
                            eof_ = true;
                            return kEof;
                        } else if(buffer_.size() < kBlockSize) {
                            // 实际读取的数据小于block大小，表明已经读完了
                            eof_ = true;
                        }

                        continue;
                    } else {
                        buffer_.clear();
                        return kEof;
                    }
                }

                // 解析header
                const char* header = buffer_.data();
                // 读取length的两个字节, 第5-6个字节（注意这里是小端序）
                const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
                const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
                const uint32_t length = a | (b << 8); 
                // 读取record类型
                const unsigned int type = header[6];
                // 超出长度，报错
                if(kHeaderSize + length > buffer_.size()) {
                    size_t drop_size = buffer_.size();
                    buffer_.clear();
                    if(!eof_) {
                        ReportCorruption(drop_size, "bad record length");
                        return kBadRecord;
                    }
                    return kEof;
                }

                if(type == kZeroType && length == 0) {
                    buffer_.clear();
                    return kBadRecord;
                }

                // 检查crc
                if(checksum_) {
                    // 获取数据中的检验和
                    // header的前4个字节是检验和
                    uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
                    // 计算获取到的数据的实际检验和
                    uint32_t actual_crc = crc32c::Value(header + 6, 1+ length);
                    if(actual_crc != expected_crc) {
                        size_t drop_size = buffer_.size();
                        buffer_.clear();
                        ReportCorruption(drop_size, "checksum mismatch");
                        return kBadRecord;
                    }
                }
                
                // 删除读到的数据
                buffer_.remove_prefix(kHeaderSize + length);

                if(end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length < initial_offset_) {
                    result->clear();
                    return kBadRecord;
                }

                *result = Slice(header + kHeaderSize, length);
                return type;
                
            }
        }

    } // end namespace leveldb
} // end namespace leveldb 