#ifndef FORMAT_H_
#define FORMAT_H_

#include <cstdint>
#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/options.h"
//#include "leveldb/table_builder.h"

namespace leveldb {

    class Block;
    class RandomAccessFile;
    struct ReadOptions;

    // BlockHandler起到类似指针的作用，其中保存了block的位置和block的大小
    class BlockHandle {

        public:
        // BlockHandle的最长编码长度
        enum { kMaxEncodedLength = 10 + 10 };

        BlockHandle();
        // 获取block 在文件中的偏移位置
        uint64_t offset() { return offset_; }
        // 设置位置偏移
        void set_offset(uint64_t offset) { offset_ = offset; }

        // 获取block 的大小
        uint64_t size() const { return size_; }
        void set_size(uint64_t size) { size_ = size; }
        // 将offset和size编码到*dst
        void EncodeTo(std::string* dst) const;
        // 从*input中解码数据到offset和size
        Status DecodeFrom(Slice* input);
    
        private:

        // 指向block 在文件中的偏移
        uint64_t offset_;
        // block 的大小
        uint64_t size_;
    };

    // SStable文件的最后存储了一个Footer，记录了两个重要的block的BlockHandle，
    // 也即MetaIndex block和index block的BlockHandle，BlockHandle起到类似指针的作用
    class Footer {

        public:
        // Footer的编码长度，包括两个block handle和一个magic number
        enum { kEncodeLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

        Footer() = default;

        // 获取metaindex block 和 index block的handler
        const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
        const BlockHandle& index_handle() const { return index_handle_; }

        void set_metaidnex_handle(const BlockHandle& h) { metaindex_handle_ = h; }
        void set_index_handle(const BlockHandle& h) { index_handle_ = h; }
        // 将metaindex handle和index handle的数据编码到*dst
        void EncodeTo(std::string* dst) const;
        // 从*input中解码数据到metaindex handle和index handle
        Status DecodeFrom(Slice* input);

        private:
        BlockHandle metaindex_handle_;
        BlockHandle index_handle_;

    }; 

    // Footer中的魔法数
    // kTableMagicNumber was picked by running
    //    echo http://code.google.com/p/leveldb/ | sha1sum
    // and taking the leading 64 bits.
    static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

    // block由三部分组成：
    // block data <----- 区块存储的数据
    // type       <----- 采用的哪种压缩方式
    // crc32      <----- 检验和
    // 
    // 1 byte type + 4 byte crc32
    static const size_t kBlockTrailerSize = 5;

    // 顾名思义，Block的内容，此结构体对象用于构建block
    struct BlockContents {
        // 实际数据
        Slice data;
        // 是否能被缓存
        bool cacheable;
        // 是否是堆分配的，若是堆分配则能调用delete来删除data.data()
        bool heap_allocated;
    };

    // 根据block handle从指定的文件中读取block
    // 若读取失败则返回non-ok
    // 若读取成功，则将数据存到*result, 并返回OK
    Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, 
                     const BlockHandle& handle, BlockContents* result);


    inline BlockHandle::BlockHandle()
        : offset_(~static_cast<uint64_t>(0)), size_(~static_cast<uint64_t>(0)) {}

} // end namespace leveldb



#endif // FORMAT_H_
