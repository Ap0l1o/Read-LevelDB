//
// Created by Ap0l1o on 2022/3/23.
//

// Env是leveldb用来实现访问文件系统等操作系统功能的接口。
// 调用者可能希望在打开数据库时提供自定义的Env对象以获得良好控制。
// 例如，对文件系统操作进行限速。所有Env实现都是线程安全的，无需任何外部同步。


#ifndef LLEVELDB_ENV_H
#define LLEVELDB_ENV_H

#include <cstdint>
#include <cstdarg>
#include <string>
#include <vector>

#include "leveldb/export.h"
#include "leveldb/status.h"

// This workaround can be removed when leveldb::Env::DeleteFile is removed.
#if defined(_WIN32)
// On Windows, the method name DeleteFile (below) introduces the risk of
// triggering undefined behavior by exposing the compiler to different
// declarations of the Env class in different translation units.
//
// This is because <windows.h>, a fairly popular header file for Windows
// applications, defines a DeleteFile macro. So, files that include the Windows
// header before this header will contain an altered Env declaration.
//
// This workaround ensures that the compiler sees the same Env declaration,
// independently of whether <windows.h> was included.
#if defined(DeleteFile)
#undef DeleteFile
#define LEVELDB_DELETEFILE_UNDEFINED
#endif  // defined(DeleteFile)
#endif  // defined(_WIN32)

namespace leveldb {
    class FileLock;
    class Logger;
    class RandomAccessFile;
    class SequentialFile;
    class Slice;
    class WritableFile;

    class LEVELDB_EXPORT Env {
    public:
        Env();
        Env(const Env&) = delete;
        Env& operator=(const Env&) = delete;

        virtual ~Env();

        // 返回一个适用于当前操作系统的默认环境。
        static Env* Default();

        // 创建一个对象来顺序读指定名称的文件。
        // 成功：将指向新文件对象的指针存入*result中，并返回OK；
        // 失败：在*result中存入nullptr，并返回错误信息；
        // 当文件不存在时，应该返回NotFund的状态
        virtual Status NewSequentialFile(const std::string& fname, SequentialFile** result) = 0;

        // 创建一个文件对象来支持随机读指定名称的文件。
        // 成功：将指向文件对象的指针存入*result，并返回OK；
        // 失败：在*result中存入nullptr，返回错误信息，
        // 文件不存在时，相关实现应该返回NotFund状态
        // 返回的文件同一时间仅支持单线程访问
        virtual Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result) = 0;

        // 创建一个文件对象，来向指定名称的文件中写入数据。删除存在的任何同名文件并床架新的文件。
        // 成功：将指向文件对象的指针存入*result，并返回OK；
        // 失败：在*result中存入nullptr，并返回相应的错误信息；
        // 返回的文件同一时间仅支持单线程访问
        virtual Status NewWritableFile(const std::string& fname, WritableFile** result) = 0;

        // 根据指定文件名创建一个文件对象来追加数据（文件已存在则直接追加，不存在则先创建后追加）。
        // 成功：将指向文件对象的指针存入*result，并返回OK；
        // 失败：在*result中存入nullptr，返回错误信息；
        // 返回的文件对象同一时间仅支持单线程访问；
        // 若Env不允许追加文件，可能会返回一个IsNotSupportedError错误。
        virtual Status NewAppendableFile(const std::string& fname, WritableFile** result) = 0;

        // 文件存在则返回true
        virtual bool FileExists(const std::string& fname) = 0;

        // 将指定目录的子文件名存入*result（文件名是相对文件名）；
        // *result的原有内容被丢弃；
        virtual Status GetChildren(const std::string& dir, std::vector<std::string>* result) = 0;

        // 删除指定文件；
        // 为了支持合法的Env实现，默认实现会调用DeleteFile；
        // 更新的Env实现必须override RemoveFile并且忽视DeleteFile的存在，更新的代码
        // 在调用Env API时必须调用RemoveFile，而不是DeleteFile。
        virtual Status RemoveFile(const std::string& fname);
        // 后续的实现会删除此方法
        virtual Status DeleteFile(const std::string& fname);

        // 创建指定名称的文件目录
        virtual Status CreateDir(const std::string& dirname) = 0;

        // 删除指定目录
        virtual Status RemoveDir(const std::string& dirname);
        virtual Status DeleteDir(const std::string& dirname);

        // 将文件大小存入*file_size
        virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) = 0;

        // 将文件名从src重命名为target
        virtual Status RenameFile(const std::string& src, const std::string& target) = 0;


        // 锁定指定的文件，避免多线程对同一数据库的并发访问；
        // 失败：在*lock中存入nullptr，并返回错误信息；
        // 成功：将指向获取的锁对象的指针存入*lock，并返回OK，
        // 调用者应该调用UnlockFile(*file)来释放该锁对象，如果进程结束，锁会自动释放；
        //
        // 如果其他进程已经持有该锁，则立即结束并报错
        virtual Status LockFile(const std::string& fname, FileLock** lock) = 0;

        // 释放指定的锁对象
        virtual Status UnlockFile(FileLock* lock) = 0;


        // 在后台线程中安排运行一次(*function)(void* arg)
        virtual void Schedule(void (*function)(void* arg), void* arg) = 0;

        // 启动一个新线程，在新线程中调用(*function)(void* arg)；
        // 函数返回后销毁该线程；
        virtual void StartThread(void (*function)(void* arg), void* arg) = 0;

        // *path被设置为可以用来执行测试的临时目录
        virtual Status GetTestDirectory(std::string* path) = 0;

        // 创建并返回用于存储消息型信息的日志文件
        virtual Status NewLogger(const std::string& fname, Logger** result) = 0;

        // 返回自某个固定时间点依赖的微秒数，仅对计算时间增量有用。
        virtual uint64_t  NowMicros() = 0;

        // 将线程sleep/delay指定的微秒时间
        virtual void SleepForMicroseconds(int micros) = 0;

    };

    // 用于顺序读文件的文件抽象
    class LEVELDB_EXPORT SequentialFile {
    public:
        SequentialFile() = default;
        SequentialFile(const SequentialFile&) = delete;
        SequentialFile& operator=(const SequentialFile&) = delete;

        virtual ~SequentialFile();

        virtual Status Read(size_t n, Slice* result, char* scratch) = 0;
        virtual Status Skip(uint64_t n) = 0;
    };

    // 用于随机读取文件内容的文件抽象
    class LEVELDB_EXPORT RandomAccessFile {
    public:
        RandomAccessFile() = default;
        RandomAccessFile(const RandomAccessFile&) = delete;
        RandomAccessFile& operator=(const RandomAccessFile&) = delete;

        virtual ~RandomAccessFile();

        virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const = 0;
    };

    // 用于顺序写文件的文件抽象
    class LEVELDB_EXPORT WritableFile {
    public:
        WritableFile() = default;
        WritableFile(const WritableFile&) = delete;
        WritableFile& operator=(const WritableFile&) = delete;
        virtual ~WritableFile();

        virtual Status Append(const Slice& data) = 0;
        virtual Status Close() = 0;
        virtual Status Flush() = 0;
        virtual Status Sync() = 0;
    };

    // 用于写日志消息的接口
    class LEVELDB_EXPORT Logger {
    public:
        Logger() = default;
        Logger(const Logger&) = delete;
        Logger& operator=(const Logger&) = delete;

        virtual ~Logger();

        virtual void Logv(const char* format, std::va_list ap) = 0;
    };

    // 标识锁定的文件
    class LEVELDB_EXPORT FileLock {
    public:
        FileLock() = default;
        FileLock(const FileLock&) = delete;
        FileLock& operator=(const FileLock&) = delete;

        virtual ~FileLock();
    };

    // 如果*info_log不为空， 则将指定的数据记录到*info_log
    void Log(Logger* info_log, const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 2, 3)))
#endif
    ;

    // 将数据写入指定文件
    LEVELDB_EXPORT Status WriteStringToFile(Env* env, const Slice& data, const std::string& fname);

    // 从指定文件中读取数据
    LEVELDB_EXPORT Status ReadFileToString(Env* env, const std::string& fname, std::string* data);

    // Env的一个实现类，所有方法直接调用另一个Env的对应方法
    class LEVELDB_EXPORT EnvWrapper : public Env {
    public:
        explicit EnvWrapper(Env* t) : target_(t) {}
        virtual ~EnvWrapper();

        Env* target() const { return target_; }

        Status NewSequentialFile(const std::string& f, SequentialFile** r) override {
            return target_->NewSequentialFile(f, r);
        }

        Status NewRandomAccessFile(const std::string& f, RandomAccessFile** r) override {
            return target_->NewRandomAccessFile(f, r);
        }

        Status NewWritableFile(const std::string& f,WritableFile** r ) override {
            return target_->NewWritableFile(f, r);
        }

        Status NewAppendableFile(const std::string& f, WritableFile** r) override {
            return target_->NewAppendableFile(f, r);
        }

        bool FileExists(const std::string& f) override {
            return target_->FileExists(f);
        }

        Status GetChildren(const std::string& dir, std::vector<std::string>* r) override {
            return target_->GetChildren(dir, r);
        }

        Status RemoveFile(const std::string& f) override {
            return target_->RemoveFile(f);
        }

        Status CreateDir(const std::string& d) override {
            return target_->CreateDir(d);
        }

        Status RemoveDir(const std::string& d) override {
            return target_->RemoveDir(d);
        }

        Status GetFileSize(const std::string& f, uint64_t* s) override {
            return target_->GetFileSize(f, s);
        }

        Status RenameFile(const std::string& s, const std::string& t) override {
            return target_->RenameFile(s, t);
        }

        Status LockFile(const std::string& f, FileLock** l) override {
            return target_->LockFile(f, l);
        }

        Status UnlockFile(FileLock* l) override { return target_->UnlockFile(l); }
        void Schedule(void (*f)(void*), void* a) override {
            return target_->Schedule(f, a);
        }

        void StartThread(void (*f)(void*), void* a) override {
            return target_->StartThread(f, a);
        }

        Status GetTestDirectory(std::string* path) override {
            return target_->GetTestDirectory(path);
        }

        Status NewLogger(const std::string& fname, Logger** result) override {
            return target_->NewLogger(fname, result);
        }

        uint64_t NowMicros() override { return target_->NowMicros(); }

        void SleepForMicroseconds(int micros) override {
            target_->SleepForMicroseconds(micros);
        }

    private:
        Env* target_;
    };


} // end namespace leveldb

// This workaround can be removed when leveldb::Env::DeleteFile is removed.
// Redefine DeleteFile if it was undefined earlier.
#if defined(_WIN32) && defined(LEVELDB_DELETEFILE_UNDEFINED)
#if defined(UNICODE)
#define DeleteFile DeleteFileW
#else
#define DeleteFile DeleteFileA
#endif  // defined(UNICODE)
#endif  // defined(_WIN32) && defined(LEVELDB_DELETEFILE_UNDEFINED)


#endif //LLEVELDB_ENV_H
