//
// Created by Ap0l1o on 2022/3/24.
//

#include "leveldb/env.h"
#include <cstdarg>

// This workaround can be removed when leveldb::Env::DeleteFile is removed.
// See env.h for justification.
#if defined(_WIN32) && defined(LEVELDB_DELETEFILE_UNDEFINED)
#undef DeleteFile
#endif

namespace leveldb {

    Env::Env() = default;
    Env::~Env() = default;

    Status Env::NewAppendableFile(const std::string &fname, WritableFile **result) {
        return Status::NotSupported("NewAppendableFile", fname);
    }

    Status Env::RemoveDir(const std::string &dirname) { return DeleteDir(dirname); }
    Status Env::DeleteDir(const std::string &dirname) { return RemoveDir(dirname); }

    Status Env::RemoveFile(const std::string &fname) { return DeleteFile(fname); }
    Status Env::DeleteFile(const std::string &fname) { return RemoveFile(fname); }

    SequentialFile::~SequentialFile() = default;

    RandomAccessFile::~RandomAccessFile() = default;

    WritableFile::~WritableFile() = default;

    Logger::~Logger() = default;

    FileLock::~FileLock()  = default;

    void Log(Logger* info_log, const char* format, ...) {
        if(info_log != nullptr) {
            std::va_list ap;
            va_start(ap, format);
            info_log->Logv(format, ap);
            va_end(ap);
        }
    }

    static Status DoWriteStringToFile(Env* env, const Slice& data,
                                      const std::string& fname, bool should_sync) {
        // 创建顺序写的文件对象
        WritableFile* file;
        Status s = env->NewWritableFile(fname, &file);

        if(!s.ok()) {
            return s;
        }

        // 写入数据
        s = file->Append(data);

        if(s.ok() && should_sync) {
            s = file->Sync();
        }

        if(s.ok()) {
            s = file->Close();
        }

        delete file;
        if(!s.ok()) {
            env->RemoveFile(fname);
        }
        return s;
    }

    Status WriteStringToFile(Env* env, const Slice& data, const std::string& fname) {
        return DoWriteStringToFile(env, data, fname, false);
    }

    Status WriteStringToFileSync(Env* env, const Slice& data, const std::string& fname) {
        return DoWriteStringToFile(env, data, fname, true);
    }

    Status ReadFileToString(Env* env, const std::string& fname, std::string& data) {
        data.clear();
        // 创建顺序读的文件对象
        SequentialFile* file;
        Status s = env->NewSequentialFile(fname, &file);

        if(!s.ok()) {
            return s;
        }

        // 创建读缓存
        static const int kBufferSize = 8192; // 8K
        char* space = new char[kBufferSize];

        while(true) {
            Slice fragment;
            s = file->Read(kBufferSize, &fragment, space);
            if(!s.ok()) {
                break;
            }
            data.append(fragment.data(), fragment.size());
            if(fragment.empty()) {
                break;
            }
        }

        delete[] space;
        delete file;
        return s;
    }

    EnvWrapper::~EnvWrapper() {}

} // end namespace leveldb