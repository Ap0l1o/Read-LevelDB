//
// Created by Ap0l1o on 2022/4/5.
//

#ifndef LLEVELDB_BUILDER_H
#define LLEVELDB_BUILDER_H

#include "leveldb/status.h"

namespace leveldb {

    struct Options;
    struct FileMetaData;

    class Env;
    class Iterator;
    class TableCache;
    class VersionEdit;

    // 根据数据输入迭代器iter，在数据库dbname中创建一个SSTable文件，将该SSTable文件的元数据信息
    // 保存在meta中。
    Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                      TableCache* table_cache, Iterator* iter, FileMetaData* meta);



} // end namespace leveldb

#endif //LLEVELDB_BUILDER_H
