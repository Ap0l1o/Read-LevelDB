#include "table/format.h"
//#include "leveldb/env.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

    void BlockHandle::EncodeTo(std::string* dst) const {
        assert(offset_ != ~static_cast<uint64_t>(0));
        assert(offset_ != ~static_cast<uint64_t>(0));
        // 将block的offset和size存到*dst
        PutVarint64(dst, offset_);
        PutVarint64(dst, size_);
    }

    Status BlockHandle::DecodeFrom(Slice* input) {
        // 若成功从input中读取offset和size则返回OK
        if(GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
            return Status::OK();
        } else {
            return Status::Corruption("bad block handle");
        }
    }
    
    // 对整个Footer进行打包存储，依次是：
    // metaindex block handle + index block handle + padding + magic number
    void Footer::EncodeTo(std::string* dst) const {
        const size_t original_size = dst->size();
        // 将两个block的handle分别存到dst
        metaindex_handle_.EncodeTo(dst);
        index_handle_.EncodeTo(dst);
        // 对dst进行填充
        dst->resize(2 * BlockHandle::kMaxEncodedLength);
        // 魔法数 8byte
        PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
        PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
        assert(dst->size() == original_size + kEncodeLength);
        (void)original_size;
    }

    Status Footer::DecodeFrom(Slice* input) {
        // 获取魔法数的指针(最后8字节)
        const char* magic_ptr = input->data() + kEncodeLength - 8;
        // 取高4byte和低4byte
        const uint32_t magic_lo = DecodeFixed32(magic_ptr);
        const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
        // 拼接魔法数
        const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) | (static_cast<uint64_t>(magic_lo)));

        if(magic != kTableMagicNumber) {
            return Status::Corruption("not an sstable (bad magic number)");
        }

        // 解析metaindex block handle
        Status result = metaindex_handle_.DecodeFrom(input);
        if(result.ok()) {
            // 成功解析后，继续解析index block handle
            result = index_handle_.DecodeFrom(input);
        }

        if(result.ok()) {
            // 获得当前block的尾指针
            const char* end = magic_ptr + 8;
            // 跳过当前块
            *input = Slice(end, input->data() + input->size() - end);
        }

        return result;
    }

    Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, 
                     const BlockHandle& handle, BlockContents* result) {
        
        result->data = Slice();
        result->cacheable = false;
        result->heap_allocated = false;

        // 从handle中解析block数据的大小
        size_t n = static_cast<size_t>(handle.size());
        // 根据block数据大小和尾部的type+crc的大小创建buffer
        char* buf = new char[n + kBlockTrailerSize];

        Slice contents;
        // 使用buf做暂存空间，contents的内部指针指向buf
        Status s = file->Read(handle.offset(), n+kBlockTrailerSize, &contents, buf);

        if(!s.ok()) {
            delete[] buf;
            return s;
        }

        if(contents.size() != n + kBlockTrailerSize) {
            delete[] buf;
            return Status::Corruption("truncated block read");
        }

        // 正常情况contets.data()是指向buf的    
        const char* data = contents.data();
        // 检查检验和(block data 和 block type的检验和)
        if(options.verify_checksums) {
            // 获取检验和
            const uint32_t crc = crc32c::Unmask(DecodeFixed64(data + n + 1));
            // 计算实际数据的检验和
            const uint32_t actual = crc32c::Value(data, n+1);
            if(actual != crc) {
                delete[] buf;
                s = Status::Corruption("block checksum mismatch");
                return s;
            }
        }

        // 根据block类型处理
        switch(data[n]) {
            case kNoCompression:
                if(data != buf) {
                    delete[] buf;
                    result->data = Slice(data, n);
                    result->cacheable = false;
                    result->heap_allocated = false;
                } else {
                    result->data = Slice(buf, n);
                    result->cacheable = true;
                    result->heap_allocated =false;
                }
                break;
            
            case kSnappyCompression:
                size_t ulength = 0;
                if(!port::Sanppy_GetUncompressedLength(data, n, &ulength)) {
                    delete[] buf;
                    return Status::Corruption("corrupted compressed block contens");
                }
                char* ubuf = new char[ulength];
                if(!port::Snappy_Uncompress(data, n, ubuf)) {
                    delete[] buf;
                    delete[] ubuf;
                    return Status::Corruption("corrupted compressed block contents");
                }

                delete[] buf;
                result->data = Slice(ubuf, ulength);
                result->cacheable = true;
                result->cacheable = true;
                break;

            default:
                delete[] buf;
                return Status::Corruption("bad block type");

        }

        return Status::OK();

    }

} // end namespace leveldb