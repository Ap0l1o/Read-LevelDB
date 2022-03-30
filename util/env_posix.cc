//
// Created by Ap0l1o on 2022/3/28.
//

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#ifndef __Fuchsia__
#include <sys/resource.h>
#endif
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "port/port_stdcxx.h"
#include "util/posix_logger.h"

namespace leveldb {

    namespace {

        int g_open_read_only_file_limit = -1;

        constexpr const int kDefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

        int g_mmap_limit = kDefaultMmapLimit;

#if defined(HAVE_O_CLOEXEC)
        constexpr const int kOpenBaseFlags = O_CLOEXEC;
#else
        constexpr const int kOpenBaseFlags = 0;
#endif // end defined(HAVE_O_CLOEXEC)

        constexpr const size_t kWritableFileBufferSize = 65536;

        Status PosixError(const std::string& context, int error_number) {
            if(error_number == ENOENT) {
                return Status::NotFound(context, std::strerror(error_number));
            } else {
                return Status::IOError(context, std::strerror(error_number));
            }
        }

        // 用于限制资源的使用，以避免资源耗尽的帮助类;
        // 目前用于限制只读的file descriptors 和 mmap file的使用；
        class Limiter {
        public:
            // 限制资源的最大数量为|max_acquires|
            Limiter(int max_acquires) : acquires_allowed_(max_acquires) {}
            Limiter(const Limiter&) = delete;
            Limiter& operator=(const Limiter&) = delete;

            // 如果有可用资源，获取它并返回true，否则返回false
            bool Acquire() {
                // 尝试获取资源
                int old_acquires_allowed =
                        acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
                // 成功获取，返回true
                if(old_acquires_allowed > 0) {
                    return true;
                }
                // 获取失败，返回false
                acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }

            // 释放一个之前通过Acquire()函数获取的资源
            void Release() {
                acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
            }
        private:
            // 可用资源的数量
            std::atomic<int> acquires_allowed_;
        };

        // 通过read()函数调用实现的顺序访问文件类
        class PosixSequentialFile final : public SequentialFile {
        public:
            PosixSequentialFile(std::string filename, int fd)
                : fd_(fd), filename_(filename) {}

            // 析构函数中要根据文件描述符关闭文件
            ~PosixSequentialFile() override { close(fd_); }

            // 顺序读n个字节的数据到scratch
            Status Read(size_t n, Slice* result, char* scratch) override {
                Status status;
                while (true) {
                    // 顺序读n个字节的文件到scratch
                    ::ssize_t read_size = ::read(fd_, scratch, n);
                    // 没读到数据
                    if(read_size < 0) {
                        // 重试
                        if(errno == EINTR) {
                            continue;
                        }
                        status = PosixError(filename_, errno);
                        break;
                    }
                    // 成功读到数据，根据scratch构造Slice
                    *result = Slice(scratch, read_size);
                    break;
                }
                return status;
            }

            // 从当前位置，向前跳n个字节
            Status Skip(uint64_t n) override {
                // lseek()函数用于将指针定位到指定位置
                if(::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
                    return PosixError(filename_, errno);
                }
                return Status::OK();
            }

        private:
            // 文件描述符
            const int fd_;
            // 文件名
            const std::string filename_;
        };

        // 通过pread()函数调用实现的随机读文件类
        class PosixRandomAccessFile final : public RandomAccessFile {
        public:
            PosixRandomAccessFile(std::string filename, int fd, Limiter* fd_limiter)
                : has_permanent_fd_(fd_limiter_->Acquire()),
                  fd_(has_permanent_fd_ ? fd : -1),
                  fd_limiter_(fd_limiter),
                  filename_(std::move(filename)) {

                // 文件描述符非持久有效，此时要关闭文件，在读操作时再打开
                if(!has_permanent_fd_) {
                    assert(fd_ == -1);
                    ::close(fd_);
                }
            }

           ~PosixRandomAccessFile() override {
                // 若文件描述符是持久有效的，则关闭文件，并释放资源
                if(has_permanent_fd_) {
                    assert(fd_ != -1);
                    ::close(fd_);
                    fd_limiter_->Release();
                }
            }

            // 根据位置偏移offset读取n个字节的数据
            Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
                int fd = fd_;
                // 文件描述符非持久有效，需要打开文件
                if(!has_permanent_fd_) {
                    fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
                    if(fd < 0) {
                        return PosixError(filename_, errno);
                    }
                }

                assert(fd != -1);

                Status status;
                // 读数据到scratch
                ssize_t read_size = ::pread(fd, scratch, n, static_cast<off_t>(offset));
                // 读取失败
                if(read_size < 0) {
                    status = PosixError(filename_, errno);
                }

                // 文件描述符非持久有效，读完数据要关闭文件
                if(!has_permanent_fd_) {
                    assert(fd != fd_);
                    ::close(fd);
                }

                return status;
            }
        private:
            // 是否是持久的文件描述符，若为false，则每次读取时打开文件
            const bool has_permanent_fd_;
            // 文件描述符，若为-1，则has_permanent_fd_为false
            const int fd_;
            // 用于资源限制
            Limiter* const fd_limiter_;
            // 文件名
            const std::string filename_;
        };

        // 通过mmap()函数调用实现的随机读文件类；
        // 注：通过内存地址映射的方式实现随机读文件
        class PosixMmapReadableFile final : public RandomAccessFile {
        public:
            // mmap_base[0, length-1]指向文件filename映射到内存的内容，它必须是成功调用mmap()时返回的
            // 结果。
            PosixMmapReadableFile(std::string filename, char* mmap_base, size_t length,
                                  Limiter* mmap_limiter)
                                  : mmap_base_(mmap_base),
                                    length_(length),
                                    mmap_limiter_(mmap_limiter),
                                    filename_(std::move(filename)) {}

            ~PosixMmapReadableFile() override {
                // 解除内存映射
                ::munmap(static_cast<void*>(mmap_base_), length_);
                mmap_limiter_->Release();
            }

            // offset是读取数据的起始位置偏移，n是要读取的字节数， *result保存读取的数据
            Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
                // 超出可读范围
                if(offset + n > length_) {
                    // *result保存空值
                    *result = Slice();
                    return PosixError(filename_, EINVAL);
                }
                // 读数据到*result
                *result = Slice(mmap_base_ + offset, n);
                return Status::OK();
            }
        private:
            // 内存映射地址
            char* const mmap_base_;
            // 映射的内容大小
            const size_t length_;
            Limiter* const mmap_limiter_;
            const std::string filename_;
        };

        // 写文件
        class PosixWritableFile final : public WritableFile {
        public:
            PosixWritableFile(std::string filename, int fd)
                : pos_(0),
                  fd_(fd),
                  is_manifest_(IsManifest(filename)),
                  filename_(std::move(filename)),
                  dirname_(Dirname(filename)) {}

            ~PosixWritableFile() override {
                if(fd_ >= 0) {
                    Close();
                }
            }

            Status Append(const Slice& data) override {

                size_t write_size = data.size();
                const char* write_data = data.data();

                // 计算可复制到buf_的数据量
                size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
                // 复制到buf_
                std::memcpy(buf_ + pos_, write_data, copy_size);
                // 更新相关参数
                write_data += copy_size;
                write_size -= copy_size;
                pos_ += copy_size;

                // 成功全部装入buf_
                if(write_size == 0) {
                    return Status::OK();
                }
                // 因为buf_容量不够导致还有数据没写入，则刷新buf_，再尝试写buf_
                Status status = FlushBuffer();
                if(!status.ok()) {
                    return status;
                }
                // 剩下的数据全部可以装入buf_
                if(write_size < kWritableFileBufferSize) {
                    std::memcpy(buf_, write_data, write_size);
                    pos_ = write_size;
                    return Status::OK();
                }
                // buf_还是装不下，不使用buf_了，直接调用接口写数据
                return WriteUnbuffered(write_data, write_size);
            }

            Status Close() override {
                // 刷新buf_
                Status status = FlushBuffer();
                // 关闭文件
                const int close_result = ::close(fd_);
                if(close_result < 0 && status.ok()) {
                    status = PosixError(filename_, errno);
                }
                fd_ = -1;
                return status;
            }

            Status Flush() override {
                return FlushBuffer();
            }

            Status Sync() override {
                // 确保manifest记录的新文件都刷新到了文件系统中。
                Status status = SyncDirIfManifest();
                if(!status.ok()) {
                    return status;
                }
                status = FlushBuffer();
                if(!status.ok()) {
                    return status;
                }
                return SyncFd(fd_, filename_);
            }

        private:
            // 写数据接口
            Status WriteUnbuffered(const char* data, size_t size) {
                while(size > 0) {
                    // 写入数据
                    ssize_t write_result = ::write(fd_, data, size);
                    // 写入数据失败
                    if(write_result < 0) {
                        // 重试
                        if(errno == EINTR) {
                            continue;
                        }
                        return PosixError(filename_, errno);
                    }
                    data += write_result;
                    size -= write_result;
                }
                return Status::OK();
            }

            // 将buf_中的数据刷新到文件
            Status FlushBuffer() {
                // 刷新buf_
                Status status = WriteUnbuffered(buf_, pos_);
                pos_ = 0;
                return status;
            }

            // 确保与给定的文件描述符fd_所关联的全部缓存都被刷新到持久层（磁盘），
            // 并且可以承受电源出现故障。
            static Status SyncFd(int fd, const std::string& fd_path) {
#if HAVE_FULLFSYNC
                // 在macOS和iOS系统中，fsync()并不能保证持久化能承受电源故障
                // fcntl(F_FULLFSYNC)便是用于保证这一点。
                if(::fcntl(fd, F_FULLFSYNC) == 0) {
                    return Status::OK();
                }
#endif // HAVE_FULLFSYNC

#if HAVE_FDATASYNC
                bool sync_success = ::fdatasync(fd) == 0;
#else
                bool sync_success = ::fsync(fd) == 0;
#endif // HAVE_FDATASYNC

                if(sync_success) {
                    return Status::OK();
                }

                return PosixError(fd_path, errno);

            }

            Status SyncDirIfManifest() {
                Status status;
                if(!is_manifest_) {
                    return status;
                }

                int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);
                if(fd < 0) {
                    status = PosixError(dirname_, errno);
                } else {
                    status = SyncFd(fd, dirname_);
                    ::close(fd);
                }
                return status;
            }

            // 返回文件filename的文件目录；
            // 如果路径中没有任何文件分割符，则返回"."
            static std::string Dirname(const std::string& filename) {
                // 找最后一个文件分隔符的位置，找不到返回的是npos
                std::string::size_type separator_pos = filename.rfind('/');
                if(separator_pos == std::string::npos) {
                    return std::string(".");
                }
                assert(filename.find('/', separator_pos + 1) == std::string::npos);

                return filename.substr(0, separator_pos);
            }

            // 提取文件名
            static Slice Basename(const std::string& filename) {
                std::string::size_type separator_pos = filename.rfind('/');
                if(separator_pos == std::string::npos) {
                    return Slice(filename);
                }

                assert(filename.find('/', separator_pos + 1) == std::string::npos);
                return Slice(filename.data() + separator_pos + 1,
                             filename.length() - separator_pos - 1);
            }

            // 给定的文件是否是一个manifest文件
            static bool IsManifest(const std::string& filename) {
                return Basename(filename).starts_with("MANIFEST");
            }


            // buf_[0, pos_-1]为要写到文件描述符fd_所对应的文件的内容
            char buf_[kWritableFileBufferSize];
            size_t pos_;
            int fd_;

            // 文件名是否以MANIFEST开头，也即是否为manifest文件
            const bool is_manifest_;
            const std::string filename_;
            // filename_的所在目录
            const std::string dirname_;
        };

        int LockOrUnlock(int fd, bool lock) {
            errno = 0;
            struct ::flock file_lock_info;
            std::memset(&file_lock_info, 0, sizeof(file_lock_info));
            file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);
            file_lock_info.l_whence= SEEK_SET;
            file_lock_info.l_start = 0;
            file_lock_info.l_len = 0;
            return ::fcntl(fd, F_SETLK, &file_lock_info);
        }

        // 此类的示例是线程安全的，因为此类对象都是不可变的，没有修改接口
        class PosixFileLock : public FileLock {
        public:
            PosixFileLock(int fd, std::string filename)
                : fd_(fd), filename_(std::move(filename)) {}

            int fd() const {
                return fd_;
            }

            const std::string& filename() const {
                return filename_;
            }
        private:
            const int fd_;
            const std::string filename_;
        };

        // 追踪被PosixEnv::LockFile()锁定的文件。
        //
        // 单独维护一个的集合，而不是依赖 fcntl(F_SETLK)，因为 fcntl(F_SETLK) 不
        // 提供任何针对同一进程的多次使用的保护。
        class PosixLockTable {
        public:
            bool Insert(const std::string& fname) LOCKS_EXCLUDED(mu_) {
                mu_.Lock();
                bool succeeded = locked_files_.insert(fname).second;
                mu_.Unlock();
                return succeeded;
            }
            void Remove(const std::string& fname) LOCKS_EXCLUDED(mu_) {
                mu_.Lock();
                locked_files_.erase(fname);
                mu_.Unlock();
            }

        private:
            port:: Mutex mu_;
            std::set<std::string> locked_files_ GUARDED_BY(mu_);
        };

        class PosixEnv : public Env {
        public:
            PosixEnv();
            ~PosixEnv() override {
                static const char msg[] =
                        "PosixEnv singleton destroyed. Unsupported behavior!\n";
                std::fwrite(msg, 1, sizeof(msg), stderr);
                std::abort();
            }

            // 以下各个New方法都是调用上面的文件读写类来创建文件读写对象

            Status NewSequentialFile(const std::string& filename,
                                     SequentialFile** result) override {
                int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
                if( fd < 0 ) {
                    *result = nullptr;
                    return PosixError(filename, errno);
                }

                *result = new PosixSequentialFile(filename, fd);
                return Status::OK();
            }

            Status NewRandomAccessFile(const std::string& filename,
                                       RandomAccessFile** result) override {
                *result = nullptr;
                int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
                if(fd < 0) {
                    return PosixError(filename, errno);
                }

                // 无法使用内存映射来随机读写，则用pread
                if(!mmap_limiter_.Acquire()) {
                    *result = new PosixRandomAccessFile(filename, fd, &fd_limiter_);
                    return Status::OK();
                }

                uint64_t file_size;
                Status status = GetFileSize(filename, &file_size);
                if(status.ok()) {
                    // 通过mmap()函数来内存映射，映射成功则返回映射区的内存起始地址
                    void* mmap_base = ::mmap(/*addr=*/nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
                    if(mmap_base != MAP_FAILED) {
                        *result = new PosixMmapReadableFile(filename,
                                                            reinterpret_cast<char*>(mmap_base),
                                                            file_size, &mmap_limiter_);
                    } else {
                        status = PosixError(filename, errno);
                    }
                }
                ::close(fd);
                if(!status.ok()) {
                    mmap_limiter_.Release();
                }
                return status;
            }

            Status NewWritableFile(const std::string& filename,
                                   WritableFile** result) override {
                int fd = ::open(filename.c_str(),
                                O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
                if(fd < 0) {
                    *result = nullptr;
                    return PosixError(filename, errno);
                }
                *result = new PosixWritableFile(filename, fd);
                return Status::OK();
            }

            Status NewAppendableFile(const std::string& filename,
                                     WritableFile** result) override {
                int fd = ::open(filename.c_str(),
                                O_APPEND | O_WRONLY |O_CREAT | kOpenBaseFlags, 0644);
                if(fd < 0) {
                    *result = nullptr;
                    return PosixError(filename, errno);
                }
                *result = new PosixWritableFile(filename, fd);
                return Status::OK();
            }

            bool FileExists(const std::string& filename) override {
                // 通过access()函数判断文件是否存在
                return ::access(filename.c_str(), F_OK) == 0;
            }

            // 将指定目录下的子文件存到向量result中
            Status GetChildren(const std::string& directory_path,
                               std::vector<std::string>* result) override {
                result->clear();
                ::DIR* dir = ::opendir(directory_path.c_str());
                if(dir == nullptr) {
                    return PosixError(directory_path, errno);
                }
                struct ::dirent* entry;
                while((entry = ::readdir(dir)) != nullptr) {
                    result->emplace_back(entry->d_name);
                }
                ::closedir(dir);
                return Status::OK();
            }

            Status RemoveFile(const std::string& filename) override {
                // 删除文件
                if(::unlink(filename.c_str()) != 0) {
                    return PosixError(filename, errno);
                }
                return Status::OK();
            }

            Status CreateDir(const std::string& dirname) override {
                if(::mkdir(dirname.c_str(), 0755) != 0) {
                    return PosixError(dirname, errno);
                }
                return Status::OK();
            }

            Status RemoveDir(const std::string& dirname) override {
                if(::rmdir(dirname.c_str()) != 0) {
                    return PosixError(dirname, errno);
                }
                return Status::OK();
            }

            Status GetFileSize(const std::string& filename, uint64_t* size) override {
                struct ::stat file_stat;
                // stat()用来将参数file_name 所指的文件状态, 复制到参数file_start所指的stat结构中
                if(::stat(filename.c_str(), &file_stat) != 0) {
                    *size = 0;
                    return PosixError(filename, errno);
                }
                *size = file_stat.st_size;
                return Status::OK();
            }

            Status RenameFile(const std::string& from, const std::string& to) override {
                if(std::rename(from.c_str(), to.c_str()) != 0) {
                    return PosixError(from, errno);
                }
                return Status::OK();
            }

            Status LockFile(const std::string& filename, FileLock** lock) override {
                *lock = nullptr;
                int fd = ::open(filename.c_str(), O_RDWR | O_CREAT |kOpenBaseFlags, 0644);
                if(fd < 0) {
                    return PosixError(filename, errno);
                }

                if(!locks_.Insert(filename)) {
                    ::close(fd);
                    return Status::IOError("lock " + filename, "already held by process");
                }

                if(LockOrUnlock(fd, true) == -1) {
                    int lock_errno = errno;
                    ::close(fd);
                    locks_.Remove(filename);
                    return PosixError("lock" + filename, lock_errno);
                }
                *lock = new PosixFileLock(fd, filename);
                return Status::OK();
            }

            Status UnlockFile(FileLock* lock) override {
                PosixFileLock* posix_file_lock = static_cast<PosixFileLock*>(lock);
                if(LockOrUnlock(posix_file_lock->fd(), false) == -1){
                    return PosixError("unlock " + posix_file_lock->filename(), errno);
                }
                locks_.Remove(posix_file_lock->filename());
                ::close(posix_file_lock->fd());
                delete posix_file_lock;
                return Status::OK();
            }

            void Schedule(void (*background_work_function)(void* background_work_arg),
                          void* background_work_arg) override;

            void StartThread(void (*thread_main)(void* thread_main_arg),
                             void* thread_main_arg) override {
                std::thread new_thread(thread_main, thread_main_arg);
                new_thread.detach();
            }

            Status GetTestDirectory(std::string* result) override {
                // 获取环境变量的内容
                const char* env = std::getenv("TEST_TMPDIR");
                if(env && env[0] != '\0') {
                    *result = env;
                } else {
                    char buf[100];
                    std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d",
                                  static_cast<int>(::geteuid()));
                    *result = buf;
                }
                CreateDir(*result);
                return Status::OK();
            }

            Status NewLogger(const std::string& filename, Logger** result) override {
                int fd = ::open(filename.c_str(),
                                O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);

                if(fd < 0) {
                    *result = nullptr;
                    return PosixError(filename, errno);
                }
                std::FILE* fp = ::fdopen(fd, "w");
                if(fp == nullptr) {
                    ::close(fd);
                    *result = nullptr;
                    return PosixError(filename, errno);
                } else {
                    *result = new PosixLogger(fp);
                    return Status::OK();
                }
            }

            uint64_t NowMicros() override {
                static constexpr uint64_t kUsecondsPerSecond = 1000000;
                struct ::timeval tv;
                ::gettimeofday(&tv, nullptr);
                return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
            }

            void SleepForMicroseconds(int micros) override {
                std::this_thread::sleep_for(std::chrono::microseconds(micros));
            }

        private:

            void BackgroundThreadMain();

            static void BackgroundThreadEntryPoint(PosixEnv* env) {
                env->BackgroundThreadMain();
            }

            // 将work item data存在一个Schedule()调用中
            //
            // 实例在线程调用Schedule()时构造，并在后台线程中使用
            struct BackgroundWorkItem {
                explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
                    : function(function), arg(arg) {}

                void (*const function)(void*);
                void* const arg;
            };

            port::Mutex background_work_mutex_;
            port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);
            bool started_background_thread_ GUARDED_BY(background_work_mutex_);

            std::queue<BackgroundWorkItem> background_work_queue_ GUARDED_BY(background_work_mutex_);

            PosixLockTable locks_;
            Limiter mmap_limiter_;
            Limiter fd_limiter_;
        };

        // 返回最大可并发的mmap数量
        int MaxMmaps() {
            return g_mmap_limit;
        }

        // 返回可保持打开的只读文件的最大数量
        int MaxOpenFiles() {
            if(g_open_read_only_file_limit >= 0) {
                return g_open_read_only_file_limit;
            }
#ifdef __Funchsia__
            g_open_read_only_file_limit = 50;
#else
            struct ::rlimit rlim;
            // 通过 getrlimit 来获取当前进程某一指定资源的限制,
            // 参数 RLIMIT_NOFILE 表示每个进程能打开的最大文件数目。
            if(::getrlimit(RLIMIT_NOFILE, &rlim)) {
                g_open_read_only_file_limit = 50;
            } else if(rlim.rlim_cur == RLIM_INFINITY) {
                g_open_read_only_file_limit = std::numeric_limits<int>::max();
            } else {
                // 可打开文件的20%设置为read-only file
                g_open_read_only_file_limit = rlim.rlim_cur / 5;
            }
#endif
            return g_open_read_only_file_limit;
        }

    } // end namespace

    PosixEnv::PosixEnv()
        : background_work_cv_(&background_work_mutex_),
          started_background_thread_(false),
          mmap_limiter_(MaxMmaps()),
          fd_limiter_(MaxOpenFiles()) {}

    void PosixEnv::Schedule(void (*background_work_function)(void *), void *background_work_arg) {
        background_work_mutex_.Lock();
        // 如果没有启动后台线程的话，则启动后台线程
        if(!started_background_thread_) {
            started_background_thread_ = true;
            std::thread background_thread(PosixEnv::BackgroundThreadEntryPoint, this);
            background_thread.detach();
        }
        // 如果队列为空，则后台线程可能会等待
        if(background_work_queue_.empty()) {
            background_work_cv_.Signal();
        }

        background_work_queue_.emplace(background_work_function, background_work_arg);
        background_work_mutex_.Unlock();
    }

    void PosixEnv::BackgroundThreadMain() {
        while(true) {
            background_work_mutex_.Lock();
            // 等待，直到有work完成
            while(background_work_queue_.empty()) {
                background_work_cv_.Wait();
            }

            assert(!background_work_queue_.empty());
            // 获取work函数
            auto background_work_function = background_work_queue_.front().function;
            // 获取work参数
            void* background_work_arg = background_work_queue_.front().arg;
            background_work_queue_.pop();
            background_work_mutex_.Unlock();
            background_work_function(background_work_arg);
        }
    }

    namespace {
        template <typename EnvType>
        class SingletonEnv {
        public:
            SingletonEnv() {
#if !defined(NDEBUG)
                env_initialized_.store(true, std::memory_order::memory_order_relaxed);
#endif // !defined(NDEBUG)
                static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                              "env_storage_ will not fit the Env");
                static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                              "env_storage_ does not meet the Env's alignment needs");
                new (&env_storage_) EnvType();
            }
            ~SingletonEnv() = default;

            SingletonEnv(const SingletonEnv&) = delete;
            SingletonEnv& operator=(const SingletonEnv&) = delete;

            Env* env() {
                return reinterpret_cast<Env*>(&env_storage_);
            }

            static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
                assert(!env_initialized_.load(std::memory_order::memory_order_relaxed));
#endif // !defined(NDEBUG
            }

        private:
            typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type env_storage_;
#if !defined(NDEBUG)
            static std::atomic<bool> env_initialized_;
#endif // !defined(NDEBUG)
        };

#if !defined(NDEBUG)
        template <typename EnvType>
        std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif // !defined(NDEBUG)
        using PosixDefaultEnv = SingletonEnv<PosixEnv>;

    } // end namespace

    void EnvPosixTestHelper::SetReadOnlyFDLimit(int limit) {
        PosixDefaultEnv::AssertEnvNotInitialized();
        g_open_read_only_file_limit = limit;
    }

    void EnvPosixTestHelper::SetReadOnlyMMapLimit(int limit) {
        PosixDefaultEnv::AssertEnvNotInitialized();
        g_mmap_limit = limit;
    }

    Env* Env::Default() {
        static PosixDefaultEnv  env_container;
        return env_container.env();
    }

} // end namespace leveldb
