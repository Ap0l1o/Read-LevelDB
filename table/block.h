// 对block的读取查询由Block类管理
#ifndef BLOCK_H_
#define BLOCK_H_

#include <cstddef>
#include <cstdint>
#include "leveldb/iterator.h"

namespace leveldb {

    struct BlockContents;
    class Comparator;

    class Block {

        public:

        explicit Block(const BlockContents& contents);
        Block(const Block&) = delete;
        Block operator=(const Block&) = delete;
        ~Block();

        size_t size() const { return size_; }
        Iterator* NewIterator(const Comparator* comparator);


        private:

        class Iter;
        // 从data_的最后4字节中解析出重启点的数量
        uint32_t NumRestarts() const;
        // block数据指针
        const char* data_;
        // block数据大小
        size_t size_;
        // 重启点偏移数组在data_中的偏移位置
        uint32_t restart_offset_;
        // block是否属于data_
        bool owned_;

    };

} // end namespace leveldb



#endif