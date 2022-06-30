//
// Created by Ap0l1o on 2022/4/6.
//

#ifndef LLEVELDB_DB_IMPL_H
#define LLEVELDB_DB_IMPL_H

#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "port/port_stdcxx.h"

namespace leveldb {

    class MemTable;
    class TableCache;
    class Version;
    class VersionSet;
    class VersionEdit;

    class DBImpl : public DB {
    public:
        DBImpl(const Options& options, const std::string& dbname);
        DBImpl(const DBImpl&) = delete;
        DBImpl& operator=(const DBImpl&) = delete;

        ~DBImpl() override;

        Status Put(const WriteOptions&, const Slice& key, const Slice& value) override;
        Status Delete(const WriteOptions&, const Slice& key) override;
        Status Write(const WriteOptions& options, WriteBatch* updates) override;
        Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;
        Iterator* NewIterator(const ReadOptions&) override;
        const Snapshot* GetSnapshot() override;
        void ReleaseSnapshot(const Snapshot* snapshot) override;
        bool GetProperty(const Slice& property, std::string* value) override;
        void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
        void CompactRange(const Slice* begin, const Slice* end) override;

        void TEST_CompactRange(int level, const Slice* begin, const Slice* end);
        Status TEST_CompactMemTable();
        Iterator* TEST_NewInternalIterator();
        int64_t TEST_MaxNextLevel0OverlappingBytes();

        void RecordReadSample(Slice key);

    private:
        friend class DB;
        struct CompactionState;
        struct Writer;

        // manual compaction 的信息
        struct ManualCompaction {
            int level;
            bool done;
            const InternalKey* begin;
            const InternalKey* end;
            // 用于追踪压缩进程
            InternalKey tmp_storage;
        };

        // 每一层的压缩状态.
        // compaction输出到level的状态为stats_[level]
        struct CompactionStats {
            CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

            void Add(const CompactionStats& c) {
                this->micros += c.micros;
                this->bytes_read += c.bytes_read;
                this->bytes_written += c.bytes_written;
            }

            int64_t micros;
            int64_t bytes_read;
            int64_t bytes_written;
        };

        Iterator* NewInternalIterator(const ReadOptions&,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed);

        Status NewDB();

        Status Recover(VersionEdit* edit, bool* save_manifest)
            EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        void MaybeIgnoreError(Status* s) const;

        // 删除那些已经不再需要的文件和过时的内存项
        void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        // 将内存写缓冲压缩到磁盘，并且切换到一个新的log-file/memtable，切换成功的话在创建一个新的descriptor.
        // 出现错误的话将错误信息记录到bg_error_
        void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                              VersionEdit* edit, SequenceNumber* max_sequence)
                              EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
                                EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Status MakeRoomForWrite(bool force) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
        WriteBatch* BuildBatchGroup(Writer** last_write) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        void RecordBackgroundError(const Status& s);

        void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
        static void BGWork(void* db);
        void BackgroundCall();
        void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
        void CleanupCompaction(CompactionState* compact)
            EXCLUSIVE_LOCKS_REQUIRED(mutex_);
        Status DoCompactionWork(CompactionState* compact)
            EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        Status OpenCompactionOutputFile(CompactionState* compact);
        Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
        Status InstallCompactionResults(CompactionState* compact)
            EXCLUSIVE_LOCKS_REQUIRED(mutex_);

        const Comparator* user_comparator() const {
            return internal_comparator_.user_comparator();
        }

        Env* env_;
        const InternalKeyComparator internal_comparator_;
        const InternalFilterPolicy internal_filter_policy_;
        const Options options_;
        const bool owns_info_log_;
        const bool owns_cache_;
        const std::string dbname_;

        TableCache* const table_cache_;
        FileLock* db_lock_;

        port::Mutex mutex_;
        std::atomic<bool> shutting_down_;
        port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
        MemTable* mem_;
        MemTable* imm_ GUARDED_BY(mutex_);
        std::atomic<bool> has_imm_;
        WritableFile* logfile_;
        uint64_t logfile_number_ GUARDED_BY(mutex_);
        log::Writer* log_;
        uint32_t seed_ GUARDED_BY(mutex_);

        std::deque<Writer*> writers_ GUARDED_BY(mutex_);
        WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

        SnapshotList snapshots_ GUARDED_BY(mutex_);

        std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);
        // 用于标记工作队列中是否已经有一个compaction操作
        bool background_compaction_scheduled_ GUARDED_BY(mutex_);
        ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

        VersionSet* const versions_ GUARDED_BY(mutex_);

        // 记录错误信息
        Status bg_error_ GUARDED_BY(mutex_);

        CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);
    };

}


#endif //LLEVELDB_DB_IMPL_H
