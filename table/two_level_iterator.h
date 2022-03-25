//
// Created by Ap0l1o on 2022/3/25.
//

#ifndef LLEVELDB_TWO_LEVEL_ITERATOR_H
#define LLEVELDB_TWO_LEVEL_ITERATOR_H

#include "leveldb/iterator.h"

namespace leveldb {

    struct ReadOptions;
    // 返回一个双层迭代器。
    // 一个双层迭代器包含一个索引迭代器，索引迭代器的值指向一系列data block，
    // 每个data block包含一系列KV对。
    // 返回的双层迭代器会对所有data block中的KV对按照data block的顺序进行级联拼接。
    Iterator* NewTwoLevelIterator(
            Iterator* index_iter,
            Iterator* (*block_function)(void* arg, const ReadOptions& options, const Slice& index_value) ,
            void *arg, const ReadOptions& options);

} // end namespace leveldb


#endif //LLEVELDB_TWO_LEVEL_ITERATOR_H
