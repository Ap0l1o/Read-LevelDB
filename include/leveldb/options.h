
#ifndef OPTIONS_H_
#define OPTIONS_H_


#include <cstddef>
#include "leveldb/export.h"

namespace leveldb {

    class Cache;
    class Comparator;
    class Env;
    class FilterPolicy;
    class Logger;
    class Snapshot;

    // block 中的压缩类型
    enum CompressionType {
        kNoCompression = 0x0,
        kSnappyCompression = 0x1
    };

    // 控制数据库行为的选项
    struct LEVELDB_EXPORT Options {
        // 创建使用默认值的Options
        Options();
        
        // ----------------------------------------------------------------------
        // 以下是影响数据库行为的参数

        // Comparator 用于定义table中key的排序
        // 默认：使用字典序
        const Comparator* comparator; 

        // 若为true，则数据库将会在缺失时建立
        bool create_if_missing = false;

        // 若为true，则会在数据库已存在时生成错误
        bool error_if_exists = false;

        // 若为true，则会对正在处理的数据积极检查，检测到任何错误将提前终止；
        // 这可能会导致无法预料的结果！
        bool paranoid_checks = false;

        // 用来与环境交互
        // 例如：读写文件，调度后台任务
        Env* env;

        // 数据库生成的任何非空 进度/错误信息 将会被写入到info_log，
        // 若info_log不存在的话，则会写入同一目录下的文件中
        Logger* info_log = nullptr;


        // ----------------------------------------------------------------------
        // 以下是影响数据库性能的参数


        // 内存的写缓冲区大小，数据先写到内存，后面再Flush到磁盘
        // buffer越大性能越好，特别是大量的读操作时，
        // 但在重启数据库时也会增加恢复时间
        // 默认大小为：4MB
        size_t write_buffer_size = 4 * 1024 * 1024;

        // DB最多可同时打开的文件数目
        int max_open_files = 1000;

        // 若非空，则为blocks使用特定的缓存
        // 若为空，则leveldb会自动创建并使用8MB的内部缓存
        Cache* block_cache = nullptr;

        // SStable是由block组成的，每个block的默认大小为4KB
        // 需要注意的是，block内会对数据压缩，因此实际读出的数据会小一点
        size_t block_size = 4 * 1024;

        // 两个重启点之间间隔的key数量
        // 重启点是未压缩的key-value对
        int block_restart_interval = 16;

        // 每个文件最大写入2MB，写满后转到新的文件
        // 每个block是4KB，则每个文件中有512个block
        size_t max_file_size = 2 * 1024 * 1024;

        // block内部的压缩类型
        CompressionType compression = kSnappyCompression;
    
        // EXPERIMENTAL: If true, append to existing MANIFEST and log files
        // when a database is opened.  This can significantly speed up open.
        //
        // Default: currently false, but may become true later.
        bool reuse_logs = false;

        // 若非空，则使用指定的过滤策略来减少磁盘IO
        const FilterPolicy* filter_policy = nullptr;
    }; // end struct Options

    // 控制读操作的选项
    struct LEVELDB_EXPORT ReadOptions {
        ReadOptions() = default;

        // 是否验证读取数据的检验和
        bool verify_checksums = false;

        // Should the data read for this iteration be cached in memory?
        // Callers may wish to set this field to false for bulk scans.
        // 读取数据时是否需要缓冲到内存
        bool fill_cache = true;

        // If "snapshot" is non-null, read as of the supplied snapshot
        // (which must belong to the DB that is being read and which must
        // not have been released).  If "snapshot" is null, use an implicit
        // snapshot of the state at the beginning of this read operation.
        const Snapshot* snapshot = nullptr;

    }; // end struct ReadOptions

    // 控制写操作的选项
    struct LEVELDB_EXPORT WriteOptions {
        WriteOptions() = default;
        // If true, the write will be flushed from the operating system
        // buffer cache (by calling WritableFile::Sync()) before the write
        // is considered complete.  If this flag is true, writes will be
        // slower.
        //
        // If this flag is false, and the machine crashes, some recent
        // writes may be lost.  Note that if it is just the process that
        // crashes (i.e., the machine does not reboot), no writes will be
        // lost even if sync==false.
        //
        // In other words, a DB write with sync==false has similar
        // crash semantics as the "write()" system call.  A DB write
        // with sync==true has similar crash semantics to a "write()"
        // system call followed by "fsync()".
        bool sync = false;
    }

}  // end namespace leveldb

#endif // OPTIONS_H_