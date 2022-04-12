//
// Created by Ap0l1o on 2022/4/7.
//

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

    const int kNumNonTableCacheFiles = 10;

    // 保存等待writer时的信息
    struct DBImpl::Writer {
        explicit Writer(port::Mutex* mu)
            : batch(nullptr), sync(false), done(false), cv(mu) {}

        Status status;
        WriteBatch* batch;
        bool sync;
        bool done;
        port::CondVar cv;
    };

    struct DBImpl::CompactionState {
        // compaction 所生成的文件
        struct Output {
            uint64_t number;
            uint64_t file_size;
            InternalKey smallest, largest;
        };

        Output* current_output() {
            return &outputs[outputs.size() - 1];
        }

        explicit CompactionState(Compaction* c)
            : compaction(c),
              smallest_snapshot(0),
              outfile(nullptr),
              builder(nullptr),
              total_bytes(0) {}


        Compaction* const compaction;

        // 快照的最小seq，小于此seq的entry不在服务范围内
        SequenceNumber smallest_snapshot;
        std::vector<Output> outputs;
        WritableFile* outfile;
        TableBuilder* builder;

        uint64_t total_bytes;
    };

    template <class T, class V>
    static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
        if(static_cast<V>(*ptr) > maxvalue) {
            *ptr = maxvalue;
        }
        if(static_cast<V>(*ptr) < minvalue) {
            *ptr = minvalue;
        }
    }

    Options SanitizeOptions(const std::string& dbname,
                            const InternalKeyComparator* icmp,
                            const InternalFilterPolicy* ipolicy,
                            const Options& src) {
        Options result = src;
        result.comparator = icmp;
        result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
        ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
        ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
        ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
        ClipToRange(&result.block_size, 1 << 10, 4 << 20);

        if(result.info_log == nullptr) {
            // 在与db相同的目录中打开一个日志文件
            src.env->CreateDir(dbname);
            // 将当前的log file重命名为旧log file的名字，并创建一个新的log file
            src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
            Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
            if(!s.ok()) {
                result.info_log = nullptr;
            }
        }

        if(result.block_cache == nullptr) {
            result.block_cache = NewLRUCache(8 << 20);
        }

        return result;
    }

    // 获取TableCache的大小
    static int TableCacheSize(const Options& sanitized_options) {
        // 保留十个左右的文件用于其他用途，其余的交给 TableCache。
        return sanitized_options.max_open_files - kNumNonTableCacheFiles;
    }

    DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
        : env_(raw_options.env),
          internal_comparator_(raw_options.comparator),
          internal_filter_policy_(raw_options.filter_policy),
          options_(SanitizeOptions(dbname, &internal_comparator_,
                                   &internal_filter_policy_, raw_options)),
          owns_info_log_(options_.info_log != raw_options.info_log),
          owns_cache_(options_.block_cache != raw_options.block_cache),
          dbname_(dbname),
          table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
          db_lock_(nullptr),
          shutting_down_(false),
          background_work_finished_signal_(&mutex_),
          mem_(nullptr),
          imm_(nullptr),
          has_imm_(false),
          logfile_(nullptr),
          logfile_number_(0),
          log_(nullptr),
          seed_(0),
          tmp_batch_(new WriteBatch),
          background_compaction_scheduled_(false),
          manual_compaction_(nullptr),
          versions_(new VersionSet(dbname_, &options_, table_cache_,
                                   &internal_comparator_)) {}

    DBImpl::~DBImpl() {
        // 等待后台工作完成
        mutex_.Lock();
        shutting_down_.store(true, std::memory_order_release);
        while(background_compaction_scheduled_) {
            background_work_finished_signal_.Wait();
        }
        mutex_.Unlock();
        if(db_lock_ != nullptr) {
            env_->UnlockFile(db_lock_);
        }

        delete versions_;
        if(mem_ != nullptr) {
            mem_->Unref();
        }
        if(imm_ != nullptr) {
            imm_->Unref();
        }

        delete tmp_batch_;
        delete log_;
        delete logfile_;
        delete table_cache_;

        if(owns_info_log_) {
            delete options_.info_log;
        }
        if(owns_cache_) {
            delete options_.block_cache;
        }
    }

    // 创建一个新的DB
    Status DBImpl::NewDB() {
        // 生成DB元数据，设置comparator名，log文件编号，文件编号，以及seq编号
        VersionEdit new_db;
        new_db.SetComparatorName(user_comparator()->Name());
        new_db.SetLogNumber(0);
        new_db.SetNextFile(2);
        new_db.SetLastSequence(0);

        // 创建一个新的manifest文件
        const std::string manifest = DescriptorFileName(dbname_, 1);
        WritableFile* file;
        Status s = env_->NewWritableFile(manifest, &file);
        if(!s.ok()) {
            return s;
        }

        {
            // 在manifest文件中追加记录本次操作（也即生成的元数据信息）
            log::Writer log(file);
            std::string record;
            new_db.EncodeTo(&record);
            s = log.AddRecord(record);
            if(s.ok()) {
                s = file->Sync();
            }
            if(s.ok()) {
                s = file->Close();
            }
        }

        delete file;
        if(s.ok()) {
            // 使CURRENT文件指向新的manifest文件
            s = SetCurrentFile(env_, dbname_, 1);
        } else {
            env_->RemoveFile(manifest);
        }
        return s;
    }

    void DBImpl::MaybeIgnoreError(Status *s) const {
        if(s->ok() || options_.paranoid_checks) {
            // no change needed
        } else {
            Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
            *s = Status::OK();
        }
    }

    // 删除垃圾文件
    void DBImpl::RemoveObsoleteFiles() {
        mutex_.AssertHeld();
        if(!bg_error_.ok()) {
            return;
        }

        // 将VersionSet正在使用的所有live file编号存入live集合，
        // 以确保不会删除pending文件
        std::set<uint64_t> live = pending_outputs_;
        versions_->AddLiveFiles(&live);

        // 获取当前数据的所有文件的文件名
        std::vector<std::string> filenames;
        env_->GetChildren(dbname_, &filenames);
        uint64_t number;
        FileType type;
        std::vector<std::string> files_to_delete;
        for(std::string& filename : filenames) {
            if(ParseFileName(filename, &number, &type)) {
                // 标记是否继续保存该文件
                bool keep = true;
                // 根据文件编号确定是否继续保存文件
                switch (type) {
                    case kLogFile:
                        keep = ( (number >= versions_->LogNumber()) ||
                                 (number == versions_->PrevLogNumber()) );
                        break;
                    case kDescriptorFile:
                        keep = (number >= versions_->ManifestFileNumber());
                        break;
                    case kTableFile:
                        keep = (live.find(number) != live.end());
                        break;
                    case kTempFile:
                        keep = (live.find(number) != live.end());
                        break;
                    case kCurrentFile:
                    case kDBLockFile:
                    case kInfoLogFile:
                        keep = true;
                        break;
                }

                // keep为false，也即不需要继续保存，则删除文件
                if(!keep) {
                    files_to_delete.push_back(std::move(filename));
                    if(type == kTableFile) {
                        table_cache_->Evict(number);
                    }
                    Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
                        static_cast<unsigned long long>(number));
                }
            }
        }

        mutex_.Unlock();
        for(const std::string& filename : files_to_delete) {
            env_->RemoveFile(dbname_ + "/" + filename);
        }
        mutex_.Lock();
    }

    // 从MANIFEST文件和log文件中恢复数据
    Status DBImpl::Recover(VersionEdit *edit, bool *save_manifest) {
        /**
         * 功能：
         * 1. 上次系统关闭后，元数据信息持久化到了磁盘（主要在MANIFEST文件），现在需要重新加载
         *    到内存；
         * 2. 如果上次系统关闭过程中出现了crash，但是log文件中有用来crash恢复的数据，则还要从
         *    log文件中恢复数据；
         */
        mutex_.AssertHeld();
        // 创建数据库目录
        env_->CreateDir(dbname_);
        assert(db_lock_ == nullptr);
        Status s = env_->LockFile(LockFileName(dbname_),  &db_lock_);
        if(!s.ok()) {
            return s;
        }

        // 当前DB是一个新的DB，需要创建一个新的DB
        if(!env_->FileExists(CurrentFileName(dbname_))) {
            if(options_.create_if_missing) {
                Log(options_.info_log, "Creating DB %s since it was missing.",
                    dbname_.c_str());
                s = NewDB();
                if(!s.ok()) {
                    return s;
                }
            } else {
                return Status::InvalidArgument(
                        dbname_,"dose not exists (create_if_missing is true)");
            }
        } else {
            if(options_.error_if_exists) {
                return Status::InvalidArgument(dbname_,
                                               "exists (error_if_exists is true)");
            }
        }

        // 根据MANIFEST文件执行Recover
        s = versions_->Recover(save_manifest);
        if(!s.ok()) {
            return s;
        }

        // 从那些未注册的log中恢复数据（也即系统发生crash后，从log文件中恢复数据）
        SequenceNumber max_sequence(0);
        const uint64_t min_log = versions_->LogNumber();
        const uint64_t prev_log = versions_->PrevLogNumber();
        std::vector<std::string> filenames;
        s = env_->GetChildren(dbname_, &filenames);
        if(!s.ok()) {
            return s;
        }
        std::set<uint64_t> expected;
        versions_->AddLiveFiles(&expected);
        uint64_t number;
        FileType type;
        std::vector<uint64_t> logs;
        // 找到那些用于恢复数据的log文件，存到logs中
        for(size_t i = 0; i < filenames.size(); i++) {
            if(ParseFileName(filenames[i], &number, &type)) {
                expected.erase(number);
                if(type == kLogFile && ((number >= min_log) || (number == prev_log))){
                    logs.push_back(number);
                }
            }
        }

        if(!expected.empty()) {
            char buf[50];
            std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                          static_cast<int>(expected.size()));
            return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
        }

        // 按照顺序从log中恢复数据
        std::sort(logs.begin(), logs.end());
        for(size_t i = 0; i < logs.size(); i++) {
            s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                               &max_sequence);
            if(!s.ok()) {
                return s;
            }

            versions_->MarkFileNumberUsed(logs[i]);
        }

        if(versions_->LastSequence() < max_sequence) {
            versions_->SetLastSequence(max_sequence);
        }

        return Status::OK();

    }

    // 从log文件中恢复数据
    Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log, bool *save_manifest, VersionEdit *edit,
                                  SequenceNumber *max_sequence) {
        struct LogReporter : public log::Reader::Reporter {
            Env* env;
            Logger* info_log;
            const char* fname;
            Status* status;
            void Corruption(size_t bytes, const Status& s) override {
                Log(info_log, "%s%s: dropping %d bytes; %s",
                    (this->status == nullptr ? "(ignoring error)" : ""), fname,
                    static_cast<int>(bytes), s.ToString().c_str());

                if(this->status != nullptr && this->status->ok()) {
                    *this->status = s;
                }
            }
        };

        mutex_.AssertHeld();

        // 打开log file
        // 获取log文件名
        std::string fname = LogFileName(dbname_, log_number);
        // 创建log文件的顺序读对象
        SequentialFile* file;
        Status status = env_->NewSequentialFile(fname, &file);
        if(!status.ok()) {
            MaybeIgnoreError(&status);
            return status;
        }

        // 创建一个log reader
        LogReporter reporter;
        reporter.env = env_;
        reporter.info_log = options_.info_log;
        reporter.fname = fname.c_str();
        reporter.status = (options_.paranoid_checks ? &status : nullptr);
        log::Reader reader(file, &reporter, true, 0);
        Log(options_.info_log, "Recovering log #%llu",
            (unsigned long long)log_number);

        // 从log file中读取record并添加到memtable中
        std::string scratch;
        Slice record;
        WriteBatch batch;
        int compactions = 0;
        MemTable* mem = nullptr;
        while(reader.ReadRecord(&record, &scratch) && status.ok()) {
            if(record.size() < 12) {
                reporter.Corruption(record.size(),
                                    Status::Corruption("log record too small"));
                continue;
            }

            WriteBatchInternal::SetContents(&batch, record);
            if(mem == nullptr) {
                mem = new MemTable(internal_comparator_);
                mem->Ref();
            }
            // 将WriteBatch batch的数据插入到MemTable mem中
            status = WriteBatchInternal::InsertInto(&batch, mem);
            MaybeIgnoreError(&status);
            if(!status.ok()) {
                break;
            }

            // 获取last seq  = batch的seq + batch中record的数量
            const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                            WriteBatchInternal::Count(&batch) - 1;
            if(last_log > *max_sequence) {
                *max_sequence = last_seq;
            }

            // memtable大小超过上限，需要flush到磁盘的level-0
            if(mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
                compactions++;
                *save_manifest = true;
                // 将memtable写到level-0
                status = WriteLevel0Table(mem, edit, nullptr);
                mem->Unref();
                mem = nullptr;
                if(!status.ok()) {
                    break;
                }
            }
        }
        delete file;

        // 是否应该继续重用最后一个日志文件
        if(status.ok() && options_.reuse_logs && last_log && compactions) {
            assert(logfile_ == nullptr);
            assert(log_ == nullptr);
            assert(mem_ == nullptr);
            uint64_t lfile_size;
            if(env_->GetFileSize(fname, &lfile_size).ok() &&
               env_->NewAppendableFile(fname, &logfile_).ok()) {

                Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
                log_ = new log::Writer(logfile_, lfile_size);
                logfile_number_ = log_number;
                // mem没达到存储上限，未被flush到磁盘，则对其进行重用，
                // 使mem_指向mem
                if(mem != nullptr) {
                    mem_ = mem;
                    mem = nullptr;
                } else {
                    mem_ = new MemTable(internal_comparator_);
                    mem_->Ref();
                }
            }
        }

        if(mem_ != nullptr) {
            // mem没有被重用，将其compact
            if(status.ok()) {
                *save_manifest = true;
                status = WriteLevel0Table(mem, edit, nullptr);
            }
            mem->Unref();
        }

        return status;
    }

    // 将内存memtable数据写到磁盘sstable文件中
    Status DBImpl::WriteLevel0Table(MemTable *mem, VersionEdit *edit, Version *base) {
        mutex_.AssertHeld();
        const uint64_t start_micros = env_->NowMicros();
        // 创建sstable文件的元数据信息
        FileMetaData meta;
        meta.number = versions_->NewFileNumber();
        pending_outputs_.insert(meta.number);
        // 1. 构造读取memtable的迭代器
        Iterator* iter = mem->NewIterator();
        Log(options_.info_log, "Level-0 table #%llu: started",
            (unsigned long long)meta.number);

        Status s;
        // 2. 将memtable数据写到sstable文件
        {
            mutex_.Unlock();
            s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
            mutex_.Lock();
        }

        Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
            (unsigned long long)meta.number, (unsigned long long)meta.file_size,
            s.ToString().c_str());

        delete iter;
        pending_outputs_.erase(meta.number);

        // 如果file_size为0，则表示文件已被删除，此时不应该将其加入到VersionEdit中
        int level = 0;
        if(s.ok() && meta.file_size > 0) {
            const Slice min_user_key = meta.smallest.user_key();
            const Slice max_user_key = meta.largest.user_key();
            // 3. 选择sstable的放置level，然后将该sstable的metadata添加到VersionEdit
            if(base != nullptr) {
                level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
            }
            edit->AddFile(level, meta.number, meta.file_size, meta.smallest, meta.largest);
        }

        // 4. 保存此次compaction所在level的状态
        CompactionStats stats;
        stats.micros = env_->NowMicros() - start_micros;
        stats.bytes_written = meta.file_size;
        stats_[level].Add(stats);
        return s;
    }

    void DBImpl::CompactMemTable() {
        mutex_.AssertHeld();
        assert(imm_ != nullptr);

        // 将immutable memtable写入sstable，并记录在edit中，
        VersionEdit edit;
        Version* base = versions_->current();
        base->Ref();
        Status s = WriteLevel0Table(imm_, &edit, base);
        base->Unref();

        if(s.ok() && shutting_down_.load(std::memory_order_acquire)) {
            s = Status::IOError("Deleting DB during memtable compaction");
        }

        if(s.ok()) {
            edit.SetPrevLogNumber(0);
            edit.SetLogNumber(logfile_number_);
            // 将edit应用到当前Version
            s = versions_->LogAndApply(&edit, &mutex_);
        }

        if(s.ok()) {
            imm_->Unref();
            imm_ = nullptr;
            has_imm_.store(false, std::memory_order_release);
            // 清理垃圾文件
            RemoveObsoleteFiles();
        } else {
            RecordBackgroundError(s);
        }
    }

    void DBImpl::CompactRange(const Slice *begin, const Slice *end) {
        int max_level_with_files = 1;
        {
            MutexLock l(&mutex_);
            Version* base = versions_->current();
            for(int level = 1; level < config::kNumLevels; level++) {
                if(base->OverlapInLevel(level, begin, end)) {
                    max_level_with_files = level;
                }
            }
        }
        TEST_CompactMemTable();
        for(int level = 0; level < max_level_with_files; level++) {
            TEST_CompactRange(level, begin, end);
        }
    }

    void DBImpl::TEST_CompactRange(int level, const Slice *begin, const Slice *end) {
        assert(level >= 0);
        assert(level + 1 < config::kNumLevels);

        InternalKey begin_storage, end_storage;
        ManualCompaction manual;
        manual.level = level;
        manual.done = false;
        if(begin == nullptr) {
            manual.begin = nullptr;
        } else {
            begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
            manual.begin = &begin_storage;
        }

        if(end == nullptr) {
            manual.end = nullptr;
        } else {
            end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
            manual.end = &end_storage;
        }

        MutexLock l(&mutex_);
        while(!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
              bg_error_.ok()) {
            if(manual_compaction_ == nullptr) {
                manual_compaction_ = &manual;
                MaybeScheduleCompaction();
            } else {
                background_work_finished_signal_.Wait();
            }
        }
        if(manual_compaction_ == &manual) {
            manual_compaction_ = nullptr;
        }
    }

    Status DBImpl::TEST_CompactMemTable() {
        Status s = Write(WriteOptions(), nullptr);
        if(s.ok()) {
            MutexLock l(&mutex_);
            while(imm_ != nullptr && bg_error_.ok()) {
                background_work_finished_signal_.Wait();
            }
            if(imm_ != nullptr) {
                s = bg_error_;
            }
        }
        return s;
    }

    void DBImpl::RecordBackgroundError(const Status &s) {
        mutex_.AssertHeld();
        if(bg_error_.ok()) {
            bg_error_ = s;
            background_work_finished_signal_.SignalAll();
        }
    }

    void DBImpl::MaybeScheduleCompaction() {
        // 此方法是一个递归调用

        mutex_.AssertHeld();
        if(background_compaction_scheduled_) {
            // already schedule
        } else if(!bg_error_.ok()) {
            // DB is being deleted; no more background compactions;
        } else if(!bg_error_.ok()) {
            // already got an error; no more changes
        } else if(imm_ == nullptr && manual_compaction_ == nullptr &&
                  !versions_->NeedsCompaction()) {
            // No work to be done 这是递归调用的结束点，结束条件为：
            // 1. 当前immutable memtable 为 null;
            // 2. 非手动compaction;
            // 3. VersionSet判定为不需要执行compaction;
        } else {
            background_compaction_scheduled_ = true;
            // 将BGWork放入线程池，由子线程来完成，这会进入一个递归调用
            env_->Schedule(&DBImpl::BGWork, this);
            // 这是递归调用的入口，调用链如下：
            // BGWork()->BackgroundCall()->MaybeScheduleCompaction()
        }
    }

    void DBImpl::BGWork(void *db) {
        reinterpret_cast<DBImpl*>(db)->BackgroundCall();
    }

    void DBImpl::BackgroundCall() {
        MutexLock l(&mutex_);
        assert(background_compaction_scheduled_);
        if(shutting_down_.load(std::memory_order_acquire)) {
            // no more background work when shutting down
        } else if(!bg_error_.ok()) {
            // No more background work after a background error.
        } else {
            BackgroundCompaction();
        }

        background_compaction_scheduled_ = false;
        MaybeScheduleCompaction();
        background_work_finished_signal_.SignalAll();
    }

    void DBImpl::BackgroundCompaction() {
        mutex_.AssertHeld();

        // minor compaction的触发条件
        if(imm_ != nullptr) {
            CompactMemTable();
            return ;
        }

        Compaction* c;
        bool is_manual = (manual_compaction_ != nullptr);
        InternalKey manual_end;
        if(is_manual) {
            ManualCompaction* m = manual_compaction_;
            c = versions_->CompactRange(m->level, m->begin, m->end);
            m->done = (c == nullptr);
            if(c != nullptr) {
                manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
            }
            Log(options_.info_log,
                "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
                m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
                (m->end ? m->end->DebugString().c_str() : "(end)"),
                (m->done ? "(end)" : manual_end.DebugString().c_str()) );
        } else {
            c = versions_->PickCompaction();
        }

        Status status;
        if(c == nullptr) {
            // nothing to do
        } else if (!is_manual && c->IsTrivialMove()) {
            // 将file移动到下个level
            assert(c->num_input_files(0) == 1);
            FileMetaData* f = c->input(0, 0);
            // 将compaction的结果保存在edit中
            c->edit()->RemoveFile(c->level(), f->number);
            c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                               f->smallest, f->largest);

            // 将edit应用到当前version
            status = versions_->LogAndApply(c->edit(), &mutex_);
            if(!status.ok()) {
                RecordBackgroundError(status);
            }
            VersionSet::LevelSummaryStorage tmp;
            Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
                static_cast<unsigned long long>(f->number), c->level() + 1,
                static_cast<unsigned long long>(f->file_size),
                status.ToString().c_str(), versions_->LevelSummary(&tmp));
        } else {
            CompactionState* compact = new CompactionState(c);
            status = DoCompactionWork(compact);
            if(!status.ok()) {
                RecordBackgroundError(status);
            }
            CleanupCompaction(compact);
            c->ReleaseInputs();
            RemoveObsoleteFiles();
        }

        delete c;

        if(status.ok()) {
            // done
        } else if(shutting_down_.load(std::memory_order_acquire)) {
            // Ignore compaction errors found during shutting down
        } else {
            Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
        }

        if(is_manual) {
            ManualCompaction* m = manual_compaction_;
            if(!status.ok()) {
                m->done = true;
            }
            if(!m->done) {
                m->tmp_storage = manual_end;
                m->begin = &m->tmp_storage;
            }
            manual_compaction_ = nullptr;
        }
    }

    void DBImpl::CleanupCompaction(CompactionState *compact) {
        mutex_.AssertHeld();
        if(compact->builder != nullptr) {
            // May happen if we get a shutdown call in the middle of compaction
            compact->builder->Abandon();
            delete compact->builder;
        } else {
            assert(compact->outfile == nullptr);
        }

        delete compact->outfile;

        for(size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionState::Output& out = compact->outputs[i];
            pending_outputs_.erase(out.number);
        }
        delete compact;
    }

    Status DBImpl::OpenCompactionOutputFile(CompactionState *compact) {
        assert(compact != nullptr);
        assert(compact->builder == nullptr);
        uint64_t file_number;
        {
            mutex_.Lock();
            file_number = versions_->NewFileNumber();
            pending_outputs_.insert(file_number);
            CompactionState::Output out;
            out.number = file_number;
            out.smallest.Clear();
            out.largest.Clear();
            compact->outputs.push_back(out);
            mutex_.Unlock();
        }
        std::string fname = TableFileName(dbname_, file_number);
        Status s = env_->NewWritableFile(fname, &compact->outfile);
        if(s.ok()) {
            compact->builder = new TableBuilder(options_, compact->outfile);
        }
        return s;
    }

    // 完成compaction操作，将compaction的结果写到sstable文件
    Status DBImpl::FinishCompactionOutputFile(CompactionState *compact, Iterator *input) {
        assert(compact != nullptr);
        assert(compact->outfile != nullptr);
        assert(compact->builder != nullptr);

        const uint64_t output_number = compact->current_output()->number;
        assert(output_number != 0);

        Status s = input->status();
        // 获取compaction输出的sstable中的entry数量
        const uint64_t current_entries = compact->builder->NumEntries();
        if(s.ok()) {
            // 完成sstable的构建
            s = compact->builder->Finish();
        } else {
            compact->builder->Abandon();
        }

        const uint64_t current_bytes = compact->builder->FileSize();
        compact->current_output()->file_size = current_bytes;
        compact->total_bytes += current_bytes;
        delete compact->builder;
        compact->builder = nullptr;

        // 完成sstable文件的写入，并关闭sstable文件
        if(s.ok()) {
            s = compact->outfile->Sync();
        }
        if(s.ok()) {
            s = compact->outfile->Close();
        }
        delete compact->outfile;
        compact->outfile = nullptr;

        if(s.ok() && current_entries > 0) {
            Iterator* iter =
                    table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
            s = iter->status();
            delete iter;
            if(s.ok()) {
                Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
                    (unsigned long long)output_number, compact->compaction->level(),
                    (unsigned long long)current_entries,
                    (unsigned long long)current_bytes);
            }
        }

        return s;
    }

    // 将compaction的结果应用到当前version
    Status DBImpl::InstallCompactionResults(CompactionState *compact) {
        mutex_.AssertHeld();
        Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
            compact->compaction->num_input_files(0), compact->compaction->level(),
            compact->compaction->num_input_files(1), compact->compaction->level() + 1,
            static_cast<long long>(compact->total_bytes));

        // 将compaction输出添加到edit
        // 1. 先在edit中删除compaction的输入文件
        compact->compaction->AddInputDeletions(compact->compaction->edit());
        const int level = compact->compaction->level();
        for(size_t i = 0; i < compact->outputs.size(); i++) {
            // 2. 然后将compaction的输出文件添加到edit
            const CompactionState::Output& out = compact->outputs[i];
            compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                                 out.smallest, out.largest);
        }
        // 3. 最后将edit应用到当前version
        return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
    }

    // 执行Compaction
    Status DBImpl::DoCompactionWork(CompactionState *compact) {
        const uint64_t start_micros = env_->NowMicros();
        int64_t imm_micros = 0;
        Log(options_.info_log, "Compacting %d@%d + %d@%d files",
            compact->compaction->num_input_files(0), compact->compaction->level(),
            compact->compaction->num_input_files(1), compact->compaction->level() + 1);

        assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
        assert(compact->builder == nullptr);
        assert(compact->outfile == nullptr);
        if(snapshots_.empty()) {
            compact->smallest_snapshot = versions_->LastSequence();
        } else {
            compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
        }

        // 构造迭代器来读取compact的input files
        Iterator* input = versions_->MakeInputIterator(compact->compaction);
        mutex_.Unlock();
        input->SeekToFirst();
        Status status;
        ParsedInternalKey ikey;
        std::string current_user_key;
        bool has_current_user_key = false;
        SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
        // 从input files中读取输入
        while(input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
            if(has_imm_.load(std::memory_order_relaxed)) {
                const uint64_t  imm_start = env_->NowMicros();
                mutex_.Lock();
                if(imm_ != nullptr) {
                    CompactMemTable();
                    background_work_finished_signal_.SignalAll();
                }
                mutex_.Unlock();
                imm_micros += (env_->NowMicros() - imm_start);
            }

            // 从input files 的迭代器中读取internal key
            Slice key = input->key();
            if(compact->compaction->ShouldStopBefore(key) &&
               compact->builder != nullptr) {
                status = FinishCompactionOutputFile(compact, input);
                if(!status.ok()) {
                    break;
                }
            }

            // 处理key/value，将其添加到state
            bool drop = false;
            if(!ParseInternalKey(key, &ikey)) {
                current_user_key.clear();
                has_current_user_key = false;
                last_sequence_for_key = kMaxSequenceNumber;
            } else {
                if(!has_current_user_key ||
                   user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
                    // 当前key是第一次出现
                    current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                    has_current_user_key = true;
                    last_sequence_for_key = kMaxSequenceNumber;
                }

                if(last_sequence_for_key <= compact->smallest_snapshot) {
                    // 当前entry被具有相同user key的entry覆盖了，也即当前entry是一个旧数据，
                    // 被新数据覆盖了。
                    //
                    // 将该entry标记为丢弃。 (rule A)
                    drop = true;
                } else if(ikey.type == kTypeDeletion &&
                          ikey.sequence <= compact->smallest_snapshot &&
                          compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
                    // 对于当前user key：
                    // 1. 在更高的level中没有数据；
                    // 2. 在更低的level中的数据序号更大；
                    // 3. 在此循环的接下来的几次迭代中，层中的数据将在此处被压缩，具有较小序号的则会被丢弃（根据rule A）
                    drop = true;
                }
                last_sequence_for_key = ikey.sequence;
            }

#if 0
            Log(options_.info_log,
                " Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, ",
                "%d smallest_snapshot: %d",
                ikey.user_key.ToString().c_str(),
                (int)ikey.sequence, ikey.type, kTypeValue, drop,
                compact->compaction->IsBaseLevelForKey(ikey.user_key),
                (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif
            if(!drop) {
                if(compact->builder == nullptr) {
                    status = OpenCompactionOutputFile(compact);
                    if(!status.ok()) {
                        break;
                    }
                }

                if(compact->builder->NumEntries() == 0) {
                    compact->current_output()->smallest.DecodeFrom(key);
                }
                compact->current_output()->largest.DecodeFrom(key);
                // 构造sstable
                compact->builder->Add(key, input->value());

                if(compact->builder->FileSize() >=
                   compact->compaction->MaxOutputFileSize()) {
                    status = FinishCompactionOutputFile(compact, input);
                    if(!status.ok()) {
                        break;
                    }
                }
            }
            input->Next();
        }

        if(status.ok() && shutting_down_.load(std::memory_order_acquire)) {
            status = Status::IOError("Deleting DB during compaction");
        }

        if(status.ok() && compact->builder != nullptr) {
            status = FinishCompactionOutputFile(compact, input);
        }
        if(status.ok()) {
            status = input->status();
        }

        delete input;
        input = nullptr;

        CompactionStats stats;
        stats.micros = env_->NowMicros() - start_micros - imm_micros;
        for(int which = 0; which < 2; which++) {
            for(int i = 0; i< compact->compaction->num_input_files(which); i++) {
                stats.bytes_read += compact->compaction->input(which, i)->file_size;
            }
        }
        for(size_t i = 0; i < compact->outputs.size(); i++) {
            stats.bytes_written += compact->outputs[i].file_size;
        }

        mutex_.Lock();
        stats_[compact->compaction->level() + 1].Add(stats);

        if(status.ok()) {
            status = InstallCompactionResults(compact);
        }
        if(!status.ok()) {
            RecordBackgroundError(status);
        }
        VersionSet::LevelSummaryStorage tmp;
        Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
        return status;
    }

    namespace {

        // 迭代器的状态，用于保存一个迭代器都引用了哪些对象
        struct IterState {
            port::Mutex* const mu;
            Version* const version GUARDED_BY(mu);
            MemTable* const mem GUARDED_BY(mu);
            MemTable* const imm GUARDED_BY(mu);

            IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
                : mu(mutex), version(version), mem(mem), imm(imm) {}
        };

        // 清零函数，用于将一个迭代器所引用的对象的引用数都减1
        static void CleanupIteratorState(void* arg1, void* arg2) {
            IterState* state = reinterpret_cast<IterState*>(arg1);
            // 将迭代器引用对象的引用量都减1
            state->mu->Lock();
            state->mem->Unref();
            if(state->imm != nullptr) {
                state->imm->Unref();
            }
            state->version->Unref();
            state->mu->Unlock();
            delete state;
        }
    } // end namespace

    // 获取读取整个DB的MergingIterator迭代器
    Iterator* DBImpl::NewInternalIterator(const ReadOptions &options,
                                          SequenceNumber *latest_snapshot,
                                          uint32_t *seed) {
        mutex_.Lock();
        *latest_snapshot = versions_->LastSequence();

        // 收集所有需要的子迭代器
        std::vector<Iterator*> list;
        // 1. 首先是memtable的迭代器
        list.push_back(mem_->NewIterator());
        mem_->Ref();
        // 2. 然后是immutable memtable的迭代器
        if(imm_ != nullptr) {
            list.push_back(imm_->NewIterator());
            imm_->Ref();
        }
        // 3. 最后是sstable的迭代器
        versions_->current()->AddIterators(options, &list);

        // 将收集起来的子迭代器构成一个MergingIterator
        Iterator* internal_iter =
                NewMergingIterator(&internal_comparator_, &list[0], list.size());

        versions_->current()->Ref();

        // 因为迭代器会引用memtable，immutable memtable以及version，使用完后要对其引用数量减1，
        // 下面是对构造的合并迭代器注册一个清理函数，来一块把相关的引用数量减1
        IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
        internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

        *seed = ++seed_;
        mutex_.Unlock();
        return internal_iter;
    }

    Iterator* DBImpl::TEST_NewInternalIterator() {
        SequenceNumber ignored;
        uint32_t ignored_seed;
        return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
    }

    int64_t DBImpl::TEST_MaxNextLevel0OverlappingBytes() {
        MutexLock l(&mutex_);
        return versions_->MaxNextLevelOverlappingBytes();
    }

    Status DBImpl::Get(const ReadOptions &options, const Slice &key, std::string *value) {
        // s用于保存查询的状态
        Status s;
        MutexLock l(&mutex_);
        // snapshot是一个序号用于限制查询范围，也即将查询范围限制在不超过此序号的那些entry
        SequenceNumber snapshot;
        if(options.snapshot != nullptr) {
            snapshot =
                    static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
        } else {
            snapshot = versions_->LastSequence();
        }

        // 增加引用计数
        MemTable* mem = mem_;
        MemTable* imm = imm_;
        Version* current = versions_->current();
        mem->Ref();
        if(imm != nullptr) {
            imm->Ref();
        }
        current->Ref();

        bool have_stat_update = false;
        Version::GetStats stats;

        // 当读取sstable和memtable时需要上锁
        {
            mutex_.Unlock();
            LookupKey lkey(key, snapshot);
            if(mem->Get(lkey, value, &s)) {
                // 1. 查询memtable;
                // 查询完毕，在memtable中查到数据
            } else if(imm->Get(lkey, value, &s)) {
                // 2. 查询immutable memtable;
                // 查询完毕，在immtable memtable中查到数据
            } else {
                // 3. 在当前的Version中查询SSTables;
                s = current->Get(options, lkey, value, &stats);
                have_stat_update = true;
            }
            mutex_.Lock();
        }

        // 更新状态，本次查询可能会带来无效查询，而无效查询可能会触发compaction
        if(have_stat_update && current->UpdateStats(stats)) {
            MaybeScheduleCompaction();
        }

        // 减少引用计数
        mem->Unref();
        if(imm != nullptr) {
            imm->Unref();
        }
        current->Unref();

        return s;
    }

    Iterator* DBImpl::NewIterator(const ReadOptions &options) {
        SequenceNumber latest_snapshot;
        uint32_t seed;
        // 构造读取DB的MergingIterator
        Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
        // 对MergingIterator迭代器进行封装
        return NewDBIterator(this, user_comparator(), iter,
                             (options.snapshot != nullptr ?
                             static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number() :
                             latest_snapshot),
                             seed );
    }

    // 采样，检查是否会触发compact
    void DBImpl::RecordReadSample(Slice key) {
        MutexLock l(&mutex_);
        if(versions_->current()->RecordReadSample(key)) {
            MaybeScheduleCompaction();
        }
    }

    // 获取快照，也即最近一次被使用的序号
    const Snapshot* DBImpl::GetSnapshot() {
        MutexLock l(&mutex_);
        return snapshots_.New(versions_->LastSequence());
    }

    // 释放一个快照
    void DBImpl::ReleaseSnapshot(const Snapshot *snapshot) {
        MutexLock l(&mutex_);
        snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
    }

    Status DBImpl::Put(const WriteOptions &o, const Slice &key, const Slice &val) {
        return DB::Put(o, key, val);
    }

    Status DBImpl::Delete(const WriteOptions &options, const Slice &key) {
        return DB::Delete(options, key);
    }

    Status DBImpl::Write(const WriteOptions &options, WriteBatch *updates) {
        Writer w(&mutex_);
        w.batch = updates;
        w.sync = options.sync;
        w.done = false;

        // 将w加入写队列的尾部
        MutexLock l(&mutex_);
        writers_.push_back(&w);
        while(!w.done && &w != writers_.front()) {
            w.cv.Wait();
        }
        if(w.done) {
            return w.status;
        }

        Status status = MakeRoomForWrite(updates == nullptr);
        uint64_t last_sequence = versions_->LastSequence();
        Writer* last_writer = &w;
        if(status.ok() && updates != nullptr) {
            // 从写队列的首部开始选择多个writer的batch聚合为一个batch，last_writer标记还未被聚合的结束位置
            WriteBatch* write_batch = BuildBatchGroup(&last_writer);
            WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
            last_sequence += WriteBatchInternal::Count(write_batch);

            // 将batch写到log，随后写到到memtable
            {
                mutex_.Unlock();
                // 先写到log
                status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
                bool sync_error = false;
                if(status.ok() && options.sync) {
                    status = logfile_->Sync();
                    if(!status.ok()) {
                        sync_error = true;
                    }
                }
                // 再插入到memtable
                if(status.ok()) {
                    status = WriteBatchInternal::InsertInto(write_batch, mem_);
                }
                mutex_.Lock();
                if(sync_error) {
                    RecordBackgroundError(status);
                }
            }

            if(write_batch == tmp_batch_) {
                tmp_batch_->Clear();
            }

            versions_->SetLastSequence(last_sequence);
        }

        // 将那些聚合为一个batch并完成写入的write移出队列
        while(true) {
            Writer* ready = writers_.front();
            writers_.pop_front();
            if(ready != &w) {
                ready->status = status;
                ready->done = true;
                ready->cv.Signal();
            }
            if(ready == last_writer) {
                break;
            }
        }

        // 唤醒新的writer来写数据
        if(!writers_.empty()) {
            writers_.front()->cv.Signal();
        }

        return status;
    }

    // 将多个writer里的小WriteBatch合并成一个大的WriteBatch，last_writer记录最后一个
    // 加入合并的Writer
    WriteBatch* DBImpl::BuildBatchGroup(Writer **last_write) {
        mutex_.AssertHeld();
        assert(!writers_.empty());
        Writer* first = writers_.front();
        WriteBatch* result = first->batch;
        assert(result != nullptr);

        size_t size = WriteBatchInternal::ByteSize(first->batch);
        size_t max_size = 1 << 20;
        if(size <= (128 << 10)) {
            max_size = size + (128 << 10);
        }

        *last_write = first;
        std::deque<Writer*>::iterator iter = writers_.begin();
        ++iter;
        for(; iter != writers_.end(); ++iter) {
            Writer* w = *iter;
            if(w->sync && !first->sync) {
                break;
            }

            if(w->batch != nullptr) {
                size += WriteBatchInternal::ByteSize(w->batch);
                // 达到合并上限，完成合并
                if(size > max_size) {
                    break;
                }

                // 进行合并
                if(result == first->batch) {
                    // 合并到一个临时的batch中
                    result = tmp_batch_;
                    assert(WriteBatchInternal::Count(result) == 0);
                    WriteBatchInternal::Append(result, first->batch);
                }
                WriteBatchInternal::Append(result, w->batch);
            }
            // 标记刚刚合并的writer
            *last_write = w;
        }

        // 返回合并结果
        return result;
    }

    // 为Write腾出空间
    Status DBImpl::MakeRoomForWrite(bool force) {
        mutex_.AssertHeld();
        assert(!writers_.empty());
        bool allow_delay = !force;
        Status s;
        while(true) {
            if(!bg_error_.ok()) {
                s = bg_error_;
                break;
            } else if(allow_delay && versions_->NumLevelFiles(0) >= config::kL0_SlowdownWriteTriger) {
                // 如果允许延迟写入，且L0的文件数量超过kL0_SlowdownWriteTriger则延迟写入（1ms）
                mutex_.Unlock();
                env_->SleepForMicroseconds(1000);
                // 不允许第二次延迟写入，将allow_delay设置为false
                mutex_.Lock();
            } else if(!force &&
                      (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
                // 当前memtable中已没有可用空间
                break;
            } else if(imm_ != nullptr) {
                // 当前的memtable已经被写满了，但是immutable memtable正在被压缩，所以等待
                Log(options_.info_log, "Current memtable full; waiting...\n");
                background_work_finished_signal_.Wait();
            } else if(versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
                // L0中的文件数量过多
                Log(options_.info_log, "Too many L0 files; waiting...\n");
                background_work_finished_signal_.Wait();
            } else {
                // 尝试生成一个新的memtable，并将旧的memtable压缩
                // 生成新的Memtable的同时也要生成新的日志文件
                assert(versions_->PrevLogNumber());
                // 创建新的日志文件
                uint64_t new_log_number = versions_->NewFileNumber();
                WritableFile* lfile = nullptr;
                s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
                if(!s.ok()) {
                    versions_->ReuseFileNumber(new_log_number);
                    break;
                }
                delete log_;
                delete logfile_;
                logfile_ = lfile;
                logfile_number_ = new_log_number;
                log_ = new log::Writer(lfile);
                // 将旧的memtable作为immutable memtable
                imm_ = mem_;
                has_imm_.store(true, std::memory_order_release);
                mem_ = new MemTable(internal_comparator_);
                mem_->Ref();
                force = false;
                MaybeScheduleCompaction();
            }
        }

        return s;
    }

    bool DBImpl::GetProperty(const Slice &property, std::string *value) {
        value->clear();

        MutexLock l(&mutex_);
        Slice in = property;
        Slice prefix("leveldb.");
        if(!in.starts_with(prefix)) {
            return false;
        }
        in.remove_prefix(prefix.size());

        if(in.starts_with("num-files-at-level")) {
            in.remove_prefix(strlen("num-files-at-level"));
            uint64_t level;
            // 获取level
            bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
            if(!ok || level >= config::kNumLevels) {
                return false;
            } else {
                char buf[100];
                std::snprintf(buf, sizeof(buf), "%d",
                              versions_->NumLevelFiles(static_cast<int>(level)));
                *value = buf;
                return true;
            }
        } else if(in == "stats") {
            char buf[200];
            std::snprintf(buf, sizeof(buf),
                          "                               Compactions\n"
                          "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                          "--------------------------------------------------\n");
            value->append(buf);
            for(int level = 0; level < config::kNumLevels; level++) {
                int files = versions_->NumLevelFiles(level);
                if(stats_[level].micros > 0 || files > 0) {
                    std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                                  level, files, versions_->NumLevelBytes(level) / 1048576.0,
                                  stats_[level].micros / 1e6,
                                  stats_[level].bytes_read / 1048576.0,
                                  stats_[level].bytes_written / 1048576.0);
                    value->append(buf);
                }
            }
            return true;
        } else if(in == "sstables") {
            *value = versions_->current()->DebugString();
            return true;
        } else if(in == "approximate-memory-usage") {
            size_t total_usage = options_.block_cache->TotalCharge();
            if(mem_) {
                total_usage += mem_->ApproximateMemoryUsage();
            }
            if(imm_) {
                total_usage += imm_->ApproximateMemoryUsage();
            }
            char buf[50];
            std::snprintf(buf, sizeof(buf), "%llu",
                          static_cast<unsigned long long>(total_usage));
            value->append(buf);
            return true;
        }

        return false;
    }

    // 获取range所覆盖的数据量
    void DBImpl::GetApproximateSizes(const Range *range, int n, uint64_t *sizes) {
        MutexLock l(&mutex_);
        Version* v = versions_->current();
        v->Ref();

        for(int i = 0; i < n; i++) {
            // 构造internal key
            InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
            InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
            // 获取位置偏移
            uint64_t start = versions_->ApproximateOffsetOf(v, k1);
            uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
            // 获取覆盖的数据量，也即将位置偏移做差
            sizes[i] = (limit >= start ? limit - start : 0);
        }

        v->Unref();
    }

    Status DB::Put(const WriteOptions &opt, const Slice &key, const Slice &value) {
        WriteBatch batch;
        batch.Put(key, value);
        return Write(opt, &batch);
    }

    Status DB::Delete(const WriteOptions &opt, const Slice &key) {
        WriteBatch batch;
        batch.Delete(key);
        return Write(opt, &batch);
    }

    DB::~DB() = default;

    // 打开一个数据库，将数据库指针保存在dbptr
    Status DB::Open(const Options &options, const std::string &dbname, DB **dbptr) {
        *dbptr = nullptr;

        DBImpl* impl = new DBImpl(options, dbname);
        impl->mutex_.Lock();
        // 1. 从manifest文件恢复数据
        VersionEdit edit;
        bool save_manifest = false;
        Status s = impl->Recover(&edit, &save_manifest);
        // 2. 创建log file 和 memtable
        if(s.ok() && impl->mem_ == nullptr) {
            // 创建log file
            uint64_t new_log_number = impl->versions_->NewFileNumber();
            WritableFile* lfile;
            s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                             &lfile);
            if(s.ok()) {
                edit.SetLogNumber(new_log_number);
                impl->logfile_ = lfile;
                impl->logfile_number_ = new_log_number;
                impl->log_ = new log::Writer(lfile);
                // 创建memtable
                impl->mem_ = new MemTable(impl->internal_comparator_);
                impl->mem_->Ref();
            }
        }
        // 3. 应用从recovery过程中获得的VersionEdit
        if(s.ok() && save_manifest) {
            edit.SetPrevLogNumber(0);
            edit.SetLogNumber(impl->logfile_number_);
            s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
        }
        if(s.ok()) {
            // 删除旧文件
            impl->RemoveObsoleteFiles();
            // 启动compaction线程
            impl->MaybeScheduleCompaction();
        }
        impl->mutex_.Unlock();
        if(s.ok()) {
            assert(impl->mem_ != nullptr);
            *dbptr = impl;
        } else {
            delete impl;
        }
        return s;
    }

    Snapshot::~Snapshot() = default;

    Status DestroyDB(const std::string& dbname, const Options& options) {
        Env* env = options.env;
        std::vector<std::string> filenames;
        Status result = env->GetChildren(dbname, &filenames);
        if(!result.ok()) {
            return Status::OK();
        }
        FileLock* lock;
        const std::string lockname = LockFileName(dbname);
        result = env->LockFile(lockname, &lock);
        if(result.ok()) {
            uint64_t number;
            FileType type;
            for(size_t i = 0; i < filenames.size(); i++) {
                if(ParseFileName(filenames[i], &number, &type) &&
                   type != kDBLockFile) {
                    Status del = env->RemoveFile(dbname + "/" + filenames[i]);
                    if(result.ok() && !del.ok()) {
                        result = del;
                    }
                }
            }

            env->UnlockFile(lock);
            env->RemoveFile(lockname);
            env->RemoveDir(dbname);
        }
        return result;
    }

} // end namespace leveldb















