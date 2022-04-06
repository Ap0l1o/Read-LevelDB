//
// Created by Ap0l1o on 2022/4/5.
//

#ifndef LLEVELDB_DUMPFILE_H
#define LLEVELDB_DUMPFILE_H

#include <string>

#include "leveldb/env.h"
#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

    // 将文件fname中的数据以文本形式存到*dst所对应的文件，也即以可读文本形式导出一个文件，以供调试时使用。
    // 此过程会进行多次dst->Append()调用，每次调用都会传递与文件中找到的单个项目相对应的换行符终止的文本。
    //
    // 如果fname不是leveldb文件或者该文件无法读取，则返回non-ok
    LEVELDB_EXPORT Status DumpFile(Env* env, const std::string& fname,
                                   WritableFile* dst);
} // end namespace leveldb


#endif //LLEVELDB_DUMPFILE_H
