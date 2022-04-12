//
// Created by Ap0l1o on 2022/3/28.
//

#include "db/filename.h"

#include <cassert>
#include <cstdio>

#include "db/dbformat.h"
#include "leveldb/env.h"
#include "util/logging.h"

namespace leveldb {

    // 将data写入到名为fname的文件中，并执行Sync()
    Status WriteStringToFileSync(Env* env, const Slice& data, const std::string& fname);

    // 生成完整的文件名
    static std::string MakeFileName(const std::string& dbname, uint64_t number,
                                    const char* suffix) {
        char buf[100];
        // 文件名由6位的number和后缀suffix组成
        std::snprintf(buf, sizeof(buf), "/%06llu.%s",
                      static_cast<unsigned long long>(number), suffix);
        // 组合文件路径和文件名
        return dbname + buf;
    }

    std::string LogFileName(const std::string& dbname, uint64_t number) {
        assert(number > 0);
        return MakeFileName(dbname, number, "log");
    }

    std::string TableFileName(const std::string& dbname, uint64_t number) {
        assert(number > 0);
        return MakeFileName(dbname, number, "ldb");
    }

    std::string SSTTableFileName(const std::string& dbname, uint64_t number) {
        assert(number > 0);
        return MakeFileName(dbname, number, "sst");
    }

    // 生成manifest文件名
    std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
        assert(number > 0);
        char buf[100];
        // 可以看到manifest文件名也包含number，且与其他使用number的文件公用一个编号系统
        std::snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
                      static_cast<unsigned long long>(number));
        return dbname + buf;
    }

    std::string CurrentFileName(const std::string& dbname) {
        return dbname + "/CURRENT";
    }

    std::string LockFileName(const std::string& dbname) {
        return dbname + "/LOCK";
    }

    std::string TempFileName(const std::string& dbname, uint64_t number) {
        assert(number > 0);
        return MakeFileName(dbname, number, "dbtmp");
    }

    std::string InfoLogFileName(const std::string& dbname) {
        return dbname + "/LOG";
    }

    std::string OldInfoLogFileName(const std::string& dbname) {
        return dbname + "/LOG.old";
    }


    // 如果filename是一个leveldb的文件，则将其文件类型存储在*type中，文件编号存在*number中；
    // 如果成功解析此文件，则返回true，否则返回false。
    // 所有的文件格式为：
    //    dbname/CURRENT
    //    dbname/LOCK
    //    dbname/LOG
    //    dbname/LOG.old
    //    dbname/MANIFEST-[0-9]+
    //    dbname/[0-9]+.(log|sst|ldb)
    bool ParseFileName(const std::string& filename, uint64_t* number, FileType* type) {
        Slice rest(filename);
        if(rest == "CURRENT") {
            *number = 0;
            *type = kCurrentFile;
        } else if(rest == "LOCK") {
            *number = 0;
            *type = kDBLockFile;
        } else if(rest == "LOG" || rest == "LOG.old") {
            *number = 0;
            *type = kInfoLogFile;
        } else if(rest.starts_with("MANIFEST-")) {
            rest.remove_prefix(strlen("MANIFEST-"));
            uint64_t num;
            // 将「MANIFEST-」后面的数字部分(也即编号)转为uint64_t并存到&num
            if(!ConsumeDecimalNumber(&rest, &num)) {
                return false;
            }
            if(!rest.empty()) {
                return false;
            }
            *type = kDescriptorFile;
            *number = num;
        } else {
            uint64_t num;
            if(!ConsumeDecimalNumber(&rest, &num)) {
                return false;
            }
            Slice suffix = rest;
            if(suffix == Slice(".log")) {
                *type = kLogFile;
            } else if(suffix == Slice(".sst") || suffix == Slice(".ldb")) {
                *type = kTableFile;
            } else if(suffix == Slice(".dbtmp")) {
                *type = kTempFile;
            } else {
                return false;
            }
            *number = num;
        }
        return true;
    }

    // 设置CURRENT文件指向descriptor_number所对应的descriptor file
    Status SetCurrentFile(Env* env, const std::string& dbname, uint64_t descriptor_number) {
        // 获取manifest文件名
        std::string manifest = DescriptorFileName(dbname, descriptor_number);
        Slice contents = manifest;
        assert(contents.starts_with(dbname + "/"));
        // 去掉manifest文件名的前缀
        contents.remove_prefix(dbname.size() + 1);
        // 创建一个临时文件
        std::string tmp = TempFileName(dbname, descriptor_number);
        // 将新的manifest文件名写入临时文件
        Status s = WriteStringToFileSync(env, contents.ToString() + "\n", tmp);
        if(s.ok()) {
            // 将该临时文件名修改为当前数据库的CURRENT文件
            s = env->RenameFile(tmp, CurrentFileName(dbname));
        }
        if(s.ok()) {
            env->RemoveFile(tmp);
        }
        return s;
    }
} // end namespace leveldb
