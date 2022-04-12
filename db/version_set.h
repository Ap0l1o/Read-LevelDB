//
// Created by Ap0l1o on 2022/4/1.
//

#ifndef LLEVELDB_VERSION_SET_H
#define LLEVELDB_VERSION_SET_H

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "port/port_stdcxx.h"

/**
 * - 一个 Version 维护整个系统中某个时期的快照，其中包括相应时间点的所有sstable文件的元数据。
 * - VersionSet 维护了所有有效的 version，内部采用双链表的结构来维护，其中的current指向最新版本的Version。
 * - VersionEdit 维护了最近对文件的改动，包括新增的文件和删除的文件。
 * LevelDB 会触发 Compaction，能对一些文件进行清理操作，让数据更加有序，清理后的数据放到新的版本里面，
 * 而老的数据作为旧数据，最终是要清理掉的，但是如果有读事务位于旧的文件，那么暂时就不能删除。因此利用
 * 引用计数，只要一个 Verison 还活着，就不允许删除该 Verison 管理的所有文件。当一个 Version 生命周期
 * 结束，它管理的所有文件的引用计数减 1. 当一个 version 被销毁时，每个和它相关联的 file 的引用计数都会 - 1，
 * 当引用计数小于 = 0 时，file 被删除。
 */

namespace leveldb {

    namespace log {
        class Writer;
    } // end namespace log

    class Compaction;
    class Iterator;
    class MemTable;
    class TableBuilder;
    class TableCache;
    class Version;
    class VersionSet;
    class WritableFile;

    /**
     * 返回满足files[i]->largest >= key的最小索引i，
     * 若没有这样的文件，则返回files.size().
     * @param icmp
     * @param files
     * @param key
     * @return
     */
    int FindFile(const InternalKeyComparator& icmp,
                 const std::vector<FileMetaData*>& files, const Slice& key);

    /**
     * 如果files中存在与key range为[smallest_user_key, largest_user_key]有重叠
     * 的file，则返回true。
     * @param icmp
     * @param disjoint_sorted_files
     * @param files
     * @param smallest_user_key 若此值为nullptr，则表示小于DB中所有的key
     * @param largest_user_key 若此值为nullptr，则表示大于DB中所有的key
     * @return
     */
    bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                               bool disjoint_sorted_files,
                               const std::vector<FileMetaData*>& files,
                               const Slice* smallest_user_key,
                               const Slice* largest_user_key);


    class Version {
    public:
        struct GetStats {
            FileMetaData* seek_file;
            int seek_file_level;
        };

        /**
         * 向iters*中添加一系列iterator，在将它们合并时会生成当前Version的内容
         * @param iters
         */
        void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

        Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
                   GetStats* stats);

        /**
         * 向当前的stat中添加"stats"
         * @param stats
         * @return 如果需要触发一个新的compaction则返回true
         */
        bool UpdateStats(const GetStats& stats);

        /**
         * 记录在指定internal key处读取的字节样本，大概每
         * config::kReadBytesPeriod字节采样一次。
         * @param key
         * @return 如果一个compaction需要被触发，则返回true
         */
        bool RecordReadSample(Slice key);

        // 引用数量管理
        void Ref();
        void Unref();

        /**
         * 获取指定level中和[begin, end]有重叠的文件，将重叠文件放在inputs中
         * @param level
         * @param begin
         * @param end
         * @param inputs
         */
        void GetOverlappingInputs(
                int level,
                const InternalKey* begin,
                const InternalKey* end,
                std::vector<FileMetaData*>* inputs);

        /**
         * 指定的key range是否和指定的level有重叠
         * @param level
         * @param smallest_user_key
         * @param largest_user_key
         * @return 有重叠则返回true，否则返回false
         */
        bool OverlapInLevel(int level, const Slice* smallest_user_key,
                            const Slice* largest_user_key);

        /**
         * 返回应该放置覆盖范围[smallest_user_key, largest_user_key]的新memtable的compaction结果
         * 的level
         * @param smallest_user_key
         * @param largest_user_key
         * @return
         */
        int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                       const Slice& largest_user_key);

        /**
         * 返回指定level的SSTable数量
         * @param level
         * @return
         */
        int NumFiles(int level) const {
            return files_[level].size();
        }

        std::string DebugString() const;


    private:

        friend class Compaction;
        friend class VersionSet;

        class LevelFileNumIterator;

        explicit Version(VersionSet* vset)
            : vset_(vset),
              next_(this),
              prev_(this),
              refs_(0),
              file_to_compact_(nullptr),
              file_to_compact_level_(-1),
              compaction_score_(-1),
              compaction_level_(-1){}

        Version(const Version&) = delete;
        Version& operator=(const Version&) = delete;

        ~Version();

        Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

        /**
         * 查找覆盖user_key的SSTable文件，然后对查找到的SSTable文件调用函数func(arg, level, f)
         * @param user_key
         * @param internal_key
         * @param arg
         * @param func
         */
        void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                bool (*func)(void*, int , FileMetaData*));

        // 此Version所属的VersionSet
        VersionSet* vset_;
        // version之间组成一个双向链表，vset_中的current指向最新的version
        Version* next_;
        Version* prev_;
        // 此Version的引用数量
        int refs_;

        // 每一个level中的file
        std::vector<FileMetaData*> files_[config::kNumLevels];

        // 基于查询状态（也即因为无效查询次数过多所触发compact）所选择的下次要执行compact的
        // sstable文件以及其所在level
        FileMetaData* file_to_compact_;
        int file_to_compact_level_;

        // 下一次要进行compact的level的得分，得分<=1表示不是特别迫切需要进行compaction
        double compaction_score_;
        // 下一次要进行compact的level
        int compaction_level_;
    };

    class VersionSet {
    public:
        VersionSet(const std::string& dbname, const Options* options,
                   TableCache* table_cache, const InternalKeyComparator*);

        VersionSet(const VersionSet&) = delete;
        VersionSet& operator=(const VersionSet&) = delete;
        ~VersionSet();

        /**
         * 将*edit应用到当前Version以形成一个新的descriptor，该descriptor既保存在persistent state，也安装到新的
         * current version中。
         * @param edit
         * @param mu
         * @return
         */
        Status LogAndApply(VersionEdit* edit, port::Mutex* mu) EXCLUSIVE_LOCKS_REQUIRED(mu);

        // 从磁盘中恢复最后一次保存的descriptor
        Status Recover(bool* save_manifest);

        // 返回当前的version
        Version* current() const { return current_; }

        // 返回当前manifest file的编号
        uint64_t ManifestFileNumber() const { return manifest_file_number_; }

        // 分配并返回一个新的file number
        uint64_t NewFileNumber() { return next_file_number_++; }

        // 安排重复使用"file_number"，除非已经分配了新的文件号。
        void ReuseFileNumber(uint64_t file_number) {
            if(next_file_number_ = file_number + 1) {
                next_file_number_ = file_number;
            }
        }

        // 返回指定level的Table文件数量
        int NumLevelFiles(int level) const;

        // 返回指定level的所有文件的大小之和
        int64_t NumLevelBytes(int level) const;

        // 返回最后一个seq
        uint64_t LastSequence() const { return last_sequence_; }

        // 设置最后一个seq 为 s
        void SetLastSequence(uint64_t s) {
            assert(s >= last_sequence_);
            last_sequence_ = s;
        }

        // 标记指定的文件编号为used
        void MarkFileNumberUsed(uint64_t number);

        // 返回当前log file 的编号
        uint64_t LogNumber() const { return log_number_; }

        // 返回当前正在被compact的log file的编号，若不存在这样的文件，则返回0
        uint64_t PrevLogNumber() const { return prev_log_number_; }

        /**
         * 选择level和inputs来进行一次新的compaction。
         * 如果不需要进行compaction，则返回nullptr，
         * 否则返回指向描述此次compaction操作的堆分配对象指针。
         */
        Compaction* PickCompaction();

        /**
         * 返回一个在指定level上对范围[begin, end]执行compaction的compaction对象。
         * @param level
         * @param begin
         * @param end
         * @return 如果在该level上此range没有重叠则返回nullptr
         */
        Compaction* CompactRange(int level, const InternalKey* begin,
                                 const InternalKey* end);

        // 返回level >= 1 的任何文件与下一level的重叠数据的最大值（以字节为单位）。
        int64_t MaxNextLevelOverlappingBytes();

        // 创建一个迭代器来读取"*c"的 compaction input
        Iterator* MakeInputIterator(Compaction* c);

        // 返回是否需要进行一次compaction
        bool NeedsCompaction() const {
            Version* v = current_;
            return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
        }

        // 将任何有效version中的所有file存到 *live
        void AddLiveFiles(std::set<uint64_t>* live);

        // 返回key对应的数据在version v中的预估位置偏移
        uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

        struct LevelSummaryStorage {
            char buffer[100];
        };
        const char* LevelSummary(LevelSummaryStorage* scratch) const;

    private:
        class Builder;
        friend class Compaction;
        friend class Version;

        bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

        void Finalize(Version* v);

        void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                      InternalKey* largest);

        void GetRange2(const std::vector<FileMetaData*>& inputs1,
                       const std::vector<FileMetaData*>& inputs2,
                       InternalKey* smallest, InternalKey* largest);

        void SetupOtherInputs(Compaction* c);

        /**
         * 将当前内容保存到*log
         */
         Status WriteSnapshot(log::Writer* log);

         void AppendVersion(Version* v);

         Env* const env_;
         const std::string dbname_;
         const Options* const options_;
         // TableCache
         TableCache* const table_cache_;
         const InternalKeyComparator icmp_;
         // 下一个文件的编号
         uint64_t next_file_number_;
         // 当前manifest文件的编号
         uint64_t manifest_file_number_;
         // 上一个key的序号
         uint64_t last_sequence_;
         // 日志文件编号
         uint64_t log_number_;
         uint64_t prev_log_number_;

         // manifest文件
         WritableFile* descriptor_file_;
         // 对manifest文件的封装，将其封装为一个日志文件，VersionEdit序列化
         // 后会被追加写入此日志文件。在第一次创建并打开一个manifest文件时，将其
         // 指向封装为Writer的manifest文件。
         log::Writer* descriptor_log_;
         // Version双向链表的头节点
         Version dummy_versions_;
         // 指向当前最新的Version
         Version* current_;

         // 每个level的下一次compaction操作的起始key
         std::string compact_pointer_[config::kNumLevels];
    };

    class Compaction {
    public:
        ~Compaction();

        // 返回正在被compacted的level。
        // 来自level 和 level + 1的inputs会合并输出到level + 1
        int level() const { return level_; }

        // 返回记录了此次compact操作的VersionEdit对象
        VersionEdit* edit() { return &edit_; }

        // 返回指定input的大小
        int num_input_files(int which) const { return inputs_[which].size(); }
        // 返回指定的input中的file
        FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

        // 返回此次compaction所构建的文件的大小
        uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

        // 本次compaction是不是一个trivial compaction，也就是能不能通过仅将input file移动到下一个level
        // 就能完成compaction，也即不需要合并或分割。
        bool IsTrivialMove() const;

        // 将所有的inputs全部添加到此次compaction操作，作为一个delete操作，也即删除所有的input文件，
        // 并记录在*edit中。
        void AddInputDeletions(VersionEdit* edit);

        // 若能保证user_key是compaction正在输出到level+1的数据，且在更深层的level中不存在相同key，
        // 则返回true。
        bool IsBaseLevelForKey(const Slice& user_key);

        // 如果应该在处理internal_key之前停止current output，则返回true
        bool ShouldStopBefore(const Slice& internal_key);

        // compaction完成后释放input
        void ReleaseInputs();

    private:
        friend class Version;
        friend class VersionSet;
        Compaction(const Options* options, int level);

        int level_;
        uint64_t max_output_file_size_;
        Version* input_version_;
        VersionEdit edit_;

        // 两个inputs的集合，每次compaction从"level"和"level+1"中读取inputs
        std::vector<FileMetaData*> inputs_[2];

        // 用于检查重叠grandparent files的数量的状态
        // ( parent == level_ + 1, grandparent == level_ + 2 )
        std::vector<FileMetaData*> grandparents_;
        // grandparent_starts_的索引
        size_t grandparent_index_;
        // 是否已经看到部分output key
        bool seen_key_;
        // current output和grandparent file的重叠字节数
        int64_t overlapped_bytes_;

        size_t level_ptrs_[config::kNumLevels];

    };

} // end namespace leveldb

#endif //LLEVELDB_VERSION_SET_H
