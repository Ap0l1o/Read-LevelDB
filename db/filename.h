//
// Created by Ap0l1o on 2022/3/28.
//

#ifndef LLEVELDB_FILENAME_H
#define LLEVELDB_FILENAME_H

#include <cstdint>
#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"

namespace leveldb {

    class Env;

    enum FileType {
        kLogFile,
        kDBLockFile,
        kTableFile,
        kDescriptorFile,
        kCurrentFile,
        kTempFile,
        kInfoLogFile
    };

    // 返回名称为dbname的数据库中编号为number的log file的文件名；
    // 文件名的前缀为dbname
    std::string LogFileName(const std::string& dbname, uint64_t number);

    // 返回名称为dbname的数据库中编号为number的sstable的文件名；
    // 文件名的前缀为dbname
    std::string TableFileName(const std::string& dbname, uint64_t number);

    // 返回名称为dbname的数据库中编号为number的sstable的旧文件名；
    // 文件名的前缀为dbname
    std::string SSTTableFileName(const std::string& dbname, uint64_t number);

    // 返回名称为dbname的数据库中编号为number的descriptor file的文件名；
    // 文件名的前缀为dbname
    std::string DescriptionFileName(const std::string& dbname, uint64_t number);

    // 返回名称为dbname的数据中的current file的文件名，此文件包含了current manifest file的文件名；
    // 文件名的前缀为dbname
    std::string CurrentFileName(const std::string& dbname);

    // 返回名称为dbname的数据库中lock file的文件名；
    // 文件名的前缀为dbname
    std::string LockFileName(const std::string& dbname);

    // 返回名称为dbname的数据库中的temporary file name；
    // 文件名的前缀为dbname
    std::string TempFileName(const std::string& dbname, uint64_t number);

    // 返回名称为dbname的数据库中的info log file name
    std::string InfoLogFileName(const std::string& dbname);

    // 返回名称为dbname的数据库中的old info log file name
    std::string OldInfoLogFileName(const std::string& dbname);

    // 如果filename是一个leveldb的文件，则将其文件类型存储在*type中，文件编号存在*number中；
    // 如果成功解析此文件，则返回true，否则返回false。
    bool ParseFileName(const std::string& filename, uint64_t* number, FileType* type);

    // 设置CURRENT文件指向descriptor_number所对应的descriptor file
    Status SetCurrentFile(Env* env, const std::string& dbname, uint64_t descriptor_number);
}


#endif //LLEVELDB_FILENAME_H
