//
// Created by Ap0l1o on 2022/4/1.
//
#include "db/version_set.h"

#include <algorithm>
#include <cstdio>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {
    static size_t TargetFileSize(const Options* options) {
        return options->max_file_size;
    }

    // GrandParent(level + 2)的最大重叠字节数量达到多少时需要停止level -> level + 1的compaction
    static int64_t MaxGrandParentOverlapBytes(const Options* options) {
        return 10 * TargetFileSize(options);
    }

    static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
        return 25 * TargetFileSize(options);
    }

    // 每一层的最大容量，单位为字节
    static double MaxBytesForLevel(const Options* options, int level) {
        // 1048576 byte = 1024 * 1024 byte = 1MB
        // level1 = 10M，10倍递增
        double result = 10. * 1048576.0;
        while(level > 1) {
            result *= 10;
            level--;
        }
        return result;
    }

    static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
        return TargetFileSize(options);
    }

    static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
        int64_t sum = 0;
        for(size_t i=0; i <files.size(); i++) {
            sum += files[i]->file_size;
        }
        return sum;
    }

    Version::~Version() {
        // 当一个Version没有任何引用时才能被删除
        assert(refs_ == 0);

        // 从VersionSet中移除该Version节点
        prev_->next_ = next_;
        next_->prev_ = prev_;

        // 当前Version中的所有文件引用减1
        for(int level = 0; level < config::kNumLevels; level++) {
            for(size_t i = 0; i < files_[level].size(); i++) {
                FileMetaData* f = files_[level][i];
                assert(f->refs > 0);
                // 文件引用-1
                f->refs--;
                // 引用小于0就可以删除
                if(f->refs <= 0) {
                    delete f;
                }
            }
        }

    }

    // 通过二分查找，确定key可能被files中的哪个文件包含
    int FindFile(const InternalKeyComparator& icmp,
                 const std::vector<FileMetaData*>& files, const Slice& key) {
        uint32_t left = 0;
        uint32_t right = files.size();
        while(left < right) {
            uint32_t mid = (left + right) / 2;
            const FileMetaData* f = files[mid];
            if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
                // mid.largest < key , left = mid + 1
                left = mid + 1;
            } else {
                // mid.largest >= key, right = mid
                right = mid;
            }
        }
        return right;
    }

    // 指定key是否在指定文件的后面，也即指定key是否大于指定文件的largest key
    static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                          const FileMetaData* f) {
        return (user_key != nullptr && ucmp->Compare(*user_key, f->largest.user_key()) > 0);
    }

    // 指定key是否在指定文件的前面，也即指定key是否小于指定文件的smallest key
    static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                           const FileMetaData* f) {
        return (user_key != nullptr && ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
    }

    /**
     * 判断files中是否存在与[smallest, largest]重叠的file
     * @param icmp
     * @param disjoint_sorted_files
     * @param files
     * @param smallest_user_key
     * @param largest_user_key
     * @return
     */
    bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                               bool disjoint_sorted_files,
                               const std::vector<FileMetaData*>& files,
                               const Slice* smallest_user_key,
                               const Slice* largest_user_key) {

        const Comparator* ucmp = icmp.user_comparator();
        // 如果不是互不相交且有序的文件，则需要挨个文件检查
        if(!disjoint_sorted_files) {
            for(size_t i=0; i<files.size(); i++) {
                const FileMetaData* f = files[i];
                if(AfterFile(ucmp, smallest_user_key, f) ||
                   BeforeFile(ucmp, largest_user_key, f)) {
                    // 没有重叠范围
                } else {
                    // 有重叠
                    return true;
                }
            }
            return true;
        }

        // 如果互不相交且有序，则可用FindFile执行二分查询
        uint32_t index = 0;
        if(smallest_user_key != nullptr) {
            // 构造Smallest Internal Key
            InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                                  kValueTypeForSeek);
            // 查询
            index = FindFile(icmp, files, small_key.Encode());
        }
        if(index >= files.size()) {
            return false;
        }

        return !BeforeFile(ucmp, largest_user_key, files[index]);
    }

    // 一个内部迭代器，对于给定的version/level对，输出level中file的信息
    class Version::LevelFileNumIterator : public Iterator {
    public:
        LevelFileNumIterator(const InternalKeyComparator& icmp,
                             const std::vector<FileMetaData*>* flist)
              : icmp_(icmp), flist_(flist), index_(flist->size()) {

        }

        bool Valid() const override {
            return index_ < flist_->size();
        }

        void Seek(const Slice& target) override {
            index_ = FindFile(icmp_, *flist_, target);
        }

        void SeekToFirst() override {
            index_ = 0;
        }

        void SeekToLast() override {
            index_ = flist_->empty() ? 0 : flist_->size() - 1;
        }

        void Next() override {
            assert(Valid());
            index_++;
        }

        void Prev() override {
            assert(Valid());
            if(index_ == 0) {
                index_ = flist_->size();
            } else {
                index_--;
            }
        }

        // 返回当前索引指向文件的最大key
        Slice key() const override {
            assert(Valid());
            return (*flist_)[index_]->largest.Encode();
        }

        // 返回当前索引指向文件的编号和文件大小
        Slice value() const override {
            assert(Valid());
            EncodeFixed64(value_buf_, (*flist_)[index_]->number);
            EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
            return Slice(value_buf_, sizeof(value_buf_));
        }

        Status status() const override {
            return Status::OK();
        }

    private:
        const InternalKeyComparator icmp_;
        // 文件列表
        const std::vector<FileMetaData*>* const flist_;
        // 当前索引
        uint32_t index_;
        // 用作value的后端存储
        mutable char value_buf_[16];
    };

    /**
     * 返回读取指定文件的迭代器
     * @param arg
     * @param options
     * @param file_value 文件信息，用Fixed64编码的文件编号和文件大小
     * @return
     */
    static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                     const Slice& file_value) {
        TableCache* cache = reinterpret_cast<TableCache*>(arg);
        if(file_value.size() != 16) {
            return NewErrorIterator(Status::Corruption("FileReader invoked with unexpected value"));
        } else {
            return cache->NewIterator(options,
                                      DecodeFixed64(file_value.data()),
                                      DecodeFixed64(file_value.data() + 8) );
        }
    }

    // 联合两个迭代器，返回一个双层迭代器
    Iterator* Version::NewConcatenatingIterator(const ReadOptions& options, int level) const {
        // 返回一个双层迭代器
        // 第一个迭代器定位到索引，第二个迭代器根据第一个迭代器的结果定位具体的信息
        return NewTwoLevelIterator(
                new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
                vset_->table_cache_, options);
    }

    // 构造读取整个DB的iterators，并添加到*iters
    void Version::AddIterators(const ReadOptions& options, std::vector<Iterator *> *iters) {
        // 合并左右的level0的文件，因为它们之间可能有重叠
        for(size_t i = 0; i < files_[0].size(); i++) {
            iters->push_back(vset_->table_cache_->NewIterator(
                    options, files_[0][i]->number, files_[0][i]->file_size));
        }

        // 对于level > 0 ,使用一个连接的双层迭代器，来顺序遍历level中的不相交文件
        for(int level = 1; level < config::kNumLevels; level++) {
            if(!files_[level].empty()) {
                iters->push_back(NewConcatenatingIterator(options, level));
            }
        }
    }

    // 来自TableCache::Get()的回调
    namespace  {
        enum SaveState {
            kNotFound,
            kFound,
            kDelete,
            kCorrupt,
        };

        struct Saver {
            SaveState state;
            const Comparator* ucmp;
            Slice user_key;
            std::string* value;
        };

    } // end namespace

    // 将查找结果保存在v中
    static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
        Saver* s = reinterpret_cast<Saver*>(arg);
        ParsedInternalKey parsed_key;
        if(!ParseInternalKey(ikey, &parsed_key)) {
            s->state = kCorrupt;
        } else {
            // 检查key是否一致
            if(s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
                // 检查类型
                s->state = (parsed_key.type == kTypeValue) ? kFound : kDelete;
                if(s->state == kFound) {
                    s->value->assign(v.data(), v.size());
                }
            }
        }
    }

    static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
        return a->number > b->number;
    }


    // 查找文件的key range覆盖指定key的文件，然后对找到
    // 的文件调用func函数来进一步查找。
    void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void *arg,
                                     bool (*func)(void *, int, FileMetaData *)) {

        const Comparator* ucmp = vset_->icmp_.user_comparator();

        // 在当前version中查找，version的vector<FileMetaData*> files_中保存了DB中所有SSTable
        // 的元数据信息，这里需要用到的主要是文件的largest key 和 smallest key。
        //
        // 1. 在level0按newest到oldest的顺序查找，因为level0无序，所以只能顺序遍历
        std::vector<FileMetaData*> tmp;
        tmp.reserve(files_[0].size());
        for(uint32_t i = 0; i < files_[0].size(); i++) {
            FileMetaData* f = files_[0][i];
            // 检查文件的key range是否覆盖user_key，若是包含则将文件放到tmp中
            if(ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
               ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
                tmp.push_back(f);
            }
        }

        if(!tmp.empty()) {
            // 对查找到的按照文件编号进行排序
            std::sort(tmp.begin(), tmp.end(), NewestFirst);
            for(uint32_t i = 0; i < tmp.size(); i++) {
                // 对覆盖此key的file调用func函数，若函数返回false则表示不用继续查找了，
                // 返回true则表示需要继续查找。
                if(!(*func)(arg, 0, tmp[i])) {
                    return ;
                }
            }
        }

        // 2. 按照level的递增顺序，在深层level中继续查找
        for(int level = 1; level < config::kNumLevels; level++) {
            // 获取当前层的文件数量
            size_t num_files = files_[level].size();
            if(num_files == 0) continue;
            // 通过二分查找找到第一个满足largest key >= internal_key
            uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
            if(index < num_files) {
                FileMetaData* f = files_[level][index];
                if(ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
                    // 该file的key range不覆盖此key
                } else {
                    // 该file的key range覆盖此key，调用func函数，
                    // 若函数返回false则表示不用继续查找了，返回true表示继续查找
                    if(!(*func)(arg, level, f)) {
                        return ;
                    }
                }
            }
        }

    }


    // 在磁盘上执行key查找，value保存查找结果，stats保存第一次进行无效查询的文件和其所在level
    Status Version::Get(const ReadOptions& options, const LookupKey& k,
                        std::string* value, GetStats* stats) {
        stats->seek_file = nullptr;
        stats->seek_file_level = -1;

        struct State {
            Saver saver;
            GetStats* stats;
            const ReadOptions* options;
            Slice ikey;
            FileMetaData* last_file_read;
            int last_file_read_level;

            VersionSet* vset;
            Status s;
            bool found;

            /**
             * 进一步查询具体是哪个文件包含了该key
             * @param arg
             * @param level
             * @param f 所有要查询的文件
             * @return
             */
            static bool Match(void* arg, int level, FileMetaData* f) {
                State* state = reinterpret_cast<State*>(arg);

                if(state->stats->seek_file == nullptr &&
                   state->last_file_read != nullptr) {
                    // 1. 上次读取不为空，这意味着本次read操作seek了不只一个file；
                    // 2. state->stats->seek_file也为空，则意味着还没记录第一次seek的文件，
                    //    此时需要记录第一个file，如果最后返回的state->stats->seek_file不为空，
                    //    则其记录的便是第一次无效查询的文件。
                    state->stats->seek_file = state->last_file_read;
                    state->stats->seek_file_level = state->last_file_read_level;
                }

                // 记录本次读取的level和file
                state->last_file_read = f;
                state->last_file_read_level = level;
                // 在指定file中查找，查找到后调用SaveValue函数将查询结果保存到state->saver->value
                state->s = state->vset->table_cache_->Get(*state->options, f->number,
                                                          f->file_size, state->ikey,
                                                          &state->saver, SaveValue);

                if(!state->s.ok()) {
                    state->found = true;
                    return false;
                }

                switch (state->saver.state) {
                    case kNotFound:
                        // 继续去其他文件查找
                        return true;
                    case kFound:
                        state->found = true;
                        return false;
                    case kDelete:
                        return false;
                    case kCorrupt:
                        state->s = Status::Corruption("corrupted key for ", state->saver.user_key);
                        state->found = true;
                        return false;
                }

                return false;
            }
        };

        State state;
        state.found = false;
        state.stats = stats;
        state.last_file_read = nullptr;
        state.last_file_read_level = -1;

        state.options = &options;
        state.ikey = k.internal_key();
        state.vset =vset_;

        state.saver.state = kNotFound;
        state.saver.ucmp = vset_->icmp_.user_comparator();
        state.saver.user_key = k.user_key();
        state.saver.value = value;

        // 1. 找key range覆盖指定key的文件，也即找到可能包含此key的文件;
        // 2. 找到相关文件后调用Match方法在该文件中进一步查找。
        ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

        return state.found ? state.s : Status::NotFound(Slice());
    }

    // 更新一个文件的统计信息，也即其允许seek的次数
    bool Version::UpdateStats(const GetStats &stats) {
        FileMetaData* f = stats.seek_file;
        if(f != nullptr) {
            // 当一个文件的无效查询次数过多时，需要将其纳入seek compaction的备选文件；
            // 将其允许seek的次数减1，减到0时此文件便需要进行seek compaction；
            f->allowed_seeks--;
            if(f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
                file_to_compact_ = f;
                file_to_compact_level_ = stats.seek_file_level;
                return true;
            }
        }
        return false;
    }

    // 采样探测：根据internal key进行采样，计算文件潜在的无效查询次数，
    // 检查是否有文件需要被compact。
    bool Version::RecordReadSample(Slice internal_key) {
        ParsedInternalKey ikey;
        if(!ParseInternalKey(internal_key, &ikey)) {
            return false;
        }

        struct State {
            // 保存第一个key range覆盖此internal_key文件，这也意味着若要真的Get这个internal_key，
            // 则第一个进行查询的文件就是此文件。
            GetStats stats;
            // 记录找到的key range 覆盖此interval_key的文件的数量
            int matches;

            static bool Match(void* arg, int level, FileMetaData* f) {
                State* state = reinterpret_cast<State*>(arg);
                state->matches++;
                if(state->matches == 1) {
                    // 仅记录第一个匹配到，但实际查询文件却未命中key的文件
                    state->stats.seek_file = f;
                    state->stats.seek_file_level = level;
                }
                return state->matches < 2;
            }
        };

        State state;
        state.matches = 0;
        // 查找文件的key range 覆盖此internal_key的文件，对该文件调用Match函数
        ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

        // 覆盖此key的文件数量太多了，更新第一个覆盖此key的文件的统计信息
        if(state.matches >= 2) {
            return UpdateStats(state.stats);
        }

        return false;
    }

    void Version::Ref() { ++refs_; }

    void Version::Unref() {
        assert(this != &vset_->dummy_versions_);
        assert(refs_ >= 1);
        --refs_;
        if(refs_ == 0) {
            delete this;
        }
    }

    // 检查指定的level是否和指定的key range有重叠
    bool Version::OverlapInLevel(int level, const Slice *smallest_user_key, const Slice *largest_user_key) {
        return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                                     smallest_user_key, largest_user_key);
    }

    // 返回compaction输出的sstable应该放置到哪个level，compaction输出的sstable覆盖范围为[smallest_user_key, largest_user_key]
    int Version::PickLevelForMemTableOutput(const Slice &smallest_user_key, const Slice &largest_user_key) {
        int level = 0;
        // 检查是否和level 0有重叠，无重叠则往深层level放置，有重叠则直接放到level-0，并返回level-0
        if(!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
            // 如果和next level没有重叠，则放到next level，并且在之后的level的重叠字节是有限的
            InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
            InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
            std::vector<FileMetaData*> overlaps;
            // 最深放置到kMaxMemCompactLevel
            while(level < config::kMaxMemCompactLevel) {
                // 与level+1有重叠，则放置到level+1
                if(OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
                    break;
                }
                // 与level+2重叠的数量过多，则直接返回
                if(level + 2 < config::kNumLevels) {
                    GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
                    const int64_t sum = TotalFileSize(overlaps);
                    if(sum > MaxGrandParentOverlapBytes(vset_->options_)) {
                        break;
                    }
                }
                level++;
            }
        }
        return level;
    }

    // 获取level中与[begin, end]有重叠的file，将其放入*inputs
    void Version::GetOverlappingInputs(int level, const InternalKey *begin, const InternalKey *end,
                                       std::vector<FileMetaData *> *inputs) {
        assert(level >= 0);
        assert(level < config::kNumLevels);

        inputs->clear();
        Slice user_begin, user_end;
        if(begin != nullptr) {
            user_begin = begin->user_key();
        }
        if(end != nullptr) {
            user_end = end->user_key();
        }
        const Comparator* user_cmp = vset_->icmp_.user_comparator();
        for(size_t i = 0; i < files_[level].size(); ) {
            FileMetaData* f = files_[level][i++];
            const Slice file_start = f->smallest.user_key();
            const Slice file_limit = f->largest.user_key();
            if(begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
                // user_begin > file.largest_key , range在file后面，skip
            } else if(end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
                // user_end < file.smallest_key, range在file前面，skip
            } else {
                // 到了这的就是有重叠了，将文件存到inputs中
                inputs->push_back(f);
                // 如果是level0的话，因为文件之间可能有重叠，所以要检查新增加的文件是否范围更大，
                // 如果范围更大，则扩展范围并重新开始搜索。
                if(level == 0) {
                    if(begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
                        user_begin = file_start;
                        inputs->clear();
                        i = 0;
                    } else if(end != nullptr && user_cmp->Compare(file_limit, user_end) > 0) {
                        user_end = file_limit;
                        inputs->clear();
                        i = 0;
                    }
                }
            }
        }
    }

    // 打印每个level的每个文件的信息
    std::string Version::DebugString() const {
        // E.g.,
        //   --- level 1 ---
        //   17:123['a' .. 'd']
        //   20:43['e' .. 'g']
        std::string r;
        for(int level = 0; level < config::kNumLevels; level++) {
            // level行信息
            r.append("--- level ");
            AppendNumberTo(&r, level);
            r.append(" ---\n");
            const std::vector<FileMetaData*>& files = files_[level];
            // 依次输出每个文件的信息
            for(size_t i = 0; i < files.size(); i++) {
                r.push_back(' ');
                AppendNumberTo(&r, files[i]->number); // 文件编号
                r.push_back(':');
                AppendNumberTo(&r, files[i]->file_size); // 文件大小
                // 输出key range
                r.append("[");
                r.append(files[i]->smallest.DebugString());
                r.append(" .. ");
                r.append(files[i]->largest.DebugString());
                r.append("]\n");

            }
        }
        return r;
    }

    // 内部辅助类，作用如下：
    //  1. 把一个MANIFEST记录的元信息应用到版本管理器VersionSet中；
    //  2. 把当前的版本状态设置到一个Version对象中；
    class VersionSet::Builder {
    private:
        // 用于帮助按照v->files_[file_number].smallest 排序
        struct BySmallestKey {
            const InternalKeyComparator* internal_comparator;

            bool operator()(FileMetaData* f1, FileMetaData* f2) const {
                int r = internal_comparator->Compare(f1->smallest, f2->smallest);
                if(r != 0) {
                    return (r < 0);
                } else {
                    // smallest key相同则按照file number排序
                    return (f1->number < f2->number);
                }
            }
        };

        typedef std::set<FileMetaData*, BySmallestKey> FileSet;
        // 记录添加和删除文件的数据结构
        struct LevelState {
            std::set<uint64_t> delete_files;
            FileSet* added_files;
        };

        VersionSet* vset_;
        // Builder的基准Version
        Version* base_;
        // 记录每一层新增和删除的文件
        LevelState levels_[config::kNumLevels];

    public:
        Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
            base_->Ref();
            BySmallestKey cmp;
            cmp.internal_comparator = &vset_->icmp_;
            for(int level = 0; level < config::kNumLevels; level++) {
                levels_[level].added_files = new FileSet(cmp);
            }
        }

        ~Builder() {
            for(int level = 0; level < config::kNumLevels; level++) {
                const FileSet* added = levels_[level].added_files;
                std::vector<FileMetaData*> to_unref;
                to_unref.reserve(added->size());
                for(FileSet::const_iterator it = added->begin(); it != added->end(); ++it) {
                    to_unref.push_back(*it);
                }
                delete added;
                for(uint32_t i = 0; i < to_unref.size(); i++) {
                    FileMetaData* f = to_unref[i];
                    // 当一个文件的引用减到0时，删除文件
                    f->refs--;
                    if(f->refs <= 0) {
                        delete f;
                    }
                }
            }
        }

        // 将edit中的修改保存到Builder的LevelState
        void Apply(VersionEdit* edit) {
            // 更新compaction pointers
            for(size_t i = 0; i < edit->compact_pointers_.size(); i++) {
                const int level = edit->compact_pointers_[i].first;
                vset_->compact_pointer_[level] = edit->compact_pointers_[i].second.Encode().ToString();
            }
            // 删除文件
            for(const auto& deleted_file_set_kvp : edit->deleted_files_) {
                const int level = deleted_file_set_kvp.first;
                const uint64_t number = deleted_file_set_kvp.second;
                levels_[level].delete_files.insert(number);
            }
            // 添加新文件(将VersionEdit中添加的新文件添加到指定的level，这里的新文件大部分都是compaction的结果)
            /**
             * 一个文件被seek指定次数后就会被自动compact。这里假设：
             *      1. 一次seek耗时10ms；
             *      2. 每读/写1MB耗时10ms，也即读写速度为100MB/s；
             *      3. 对1MB的数据执行compaction需要读写的数据为25MB:
             *              a. 从当前level读1MB数据；
             *              b. 从next level读10-12MB数据；
             *              c. 向next level写10-12MB数据；
             * 因此，对1MB数据执行compaction的开销，相当于执行25次seek，也就意味这一次seek的开销
             * 大概相当于对40KB的数据执行compaction。从保守的角度考虑，leveldb对于每16KB的数据，
             * 允许它在触发compaction之前执行一次seek。所以，一个file在compaction之前能被执行的
             * seek次数为 number = (file_size / 16KB).
             */
            for(size_t i = 0; i < edit->new_files_.size(); i++) {
                const int level = edit->new_files_[i].first;
                FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
                f->refs = 1;
                // 16384B = 16 * 1024 B = 16KB
                f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
                if(f->allowed_seeks < 100) {
                    f->allowed_seeks = 100;
                }
                levels_[level].delete_files.erase(f->number);
                levels_[level].added_files->insert(f);
            }
        }

        // 将当前Builder保存的LevelState应用到Version *v中
        void SaveTo(Version* v) {
            BySmallestKey cmp;
            cmp.internal_comparator = &vset_->icmp_;
            for(int level = 0; level < config::kNumLevels; level++) {
                // 将新增的文件与已有的文件合并，并丢弃删除的文件，将结果存在*v中
                const std::vector<FileMetaData*>& base_files = base_->files_[level]; // 已有的文件
                std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
                std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
                const FileSet* added_files = levels_[level].added_files; // 新增的文件
                // 调整level的大小
                v->files_[level].reserve(base_files.size() + added_files->size());
                // 根据key顺序将文件合并
                for(const auto& added_file : *added_files) {
                    for(std::vector<FileMetaData*>::const_iterator bpos =
                            std::upper_bound(base_iter, base_end, added_file, cmp);
                            base_iter != bpos; ++base_iter) {
                        // 先将base_files中小于added_files的文件加入
                        MaybeAddFile(v, level, *base_iter);
                    }
                    // 将所有的added_files加入
                    MaybeAddFile(v, level, added_file);
                }

                // 将剩下的base_files加入
                for(; base_iter != base_end; ++base_iter) {
                    MaybeAddFile(v, level, *base_iter);
                }

#ifndef NDEBUG
                // 确保除了level0之外，其他的每个level之中都没有重叠文件
                if(level > 0) {
                    for(uint32_t i = 1; i < v->files_[level].size(); i++) {
                        const InternalKey& prev_end = v->files_[level][i - 1]->largest;
                        const InternalKey& this_begin = v->files_[level][i]->smallest;
                        // 确保f(i-1).largest < f(i).smallest
                        if(vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
                            std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                                         prev_end.DebugString().c_str(),
                                         this_begin.DebugString().c_str());
                            std::abort();
                        }
                    }
                }
#endif
            }
        }

        // 尝试将文件f加入到版本v的指定level中
        void MaybeAddFile(Version* v, int level, FileMetaData* f) {
            if(levels_[level].delete_files.count(f->number) > 0) {
                // 文件f在删除文件中，不做操作
            } else {
                // 获取该level的所有文件
                std::vector<FileMetaData*>* files = &v->files_[level];
                if(level > 0 && !files->empty()) {
                    // 必须与level 中的其他文件没有重叠，且满足f.smallest > f(len-1).largest
                    assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                                f->smallest) < 0);
                }
                f->refs++;
                files->push_back(f);
            }
        }
    };

    VersionSet::VersionSet(const std::string &dbname, const Options *options, TableCache *table_cache,
                           const InternalKeyComparator* cmp)
                           : env_(options->env),
                             dbname_(dbname),
                             options_(options),
                             table_cache_(table_cache),
                             icmp_(*cmp),
                             next_file_number_(2),
                             manifest_file_number_(0),
                             last_sequence_(0),
                             log_number_(0),
                             prev_log_number_(0),
                             descriptor_file_(nullptr),
                             descriptor_log_(nullptr),
                             dummy_versions_(this),
                             current_(nullptr) {

        AppendVersion(new Version(this));
    }

    VersionSet::~VersionSet() {
        current_->Unref();
        // 链表必须为空
        assert(dummy_versions_.next_ == &dummy_versions_);
        delete descriptor_log_;
        delete descriptor_file_;
    }

    // 向VersionSet中添加一个新的Version
    void VersionSet::AppendVersion(Version *v) {
        // 将这个新添加的version设置为CURRENT
        assert(v->refs_ == 0);
        assert(v != current_);
        // 当前的current不再是VersionSet的current，其引用要减1
        if(current_ != nullptr) {
            current_->Unref();
        }
        // v成为当前VersionSet的CURRENT，也即意味着
        // VersionSet引用了v，其引用加1
        current_ = v;

        // 将v添加到链表当中
        v->prev_ = dummy_versions_.prev_;
        v->next_ = &dummy_versions_;
        v->prev_->next_ = v;
        v->next_->prev_ = v;
    }

    // 在current version上应用指定的VersionEdit，生成新的MANIFEST信息，
    // 并保存到磁盘上用作current version。
    Status VersionSet::LogAndApply(VersionEdit *edit, port::Mutex *mu) {
        if(edit->has_log_number_) {
            assert(edit->log_number_ >= log_number_);
            assert(edit->log_number_ < next_file_number_);
        } else {
            edit->SetLogNumber(log_number_);
        }

        if(!edit->has_prev_log_number_) {
            edit->SetPrevLogNumber(prev_log_number_);
        }

        edit->SetNextFile(next_file_number_);
        edit->SetLastSequence(last_sequence_);

        // 1. 基于当前Version创建一个新的Version，并将VersionEdit中的改动应用到Version中
        Version* v = new Version(this);
        {
            Builder builder(this, current_);
            builder.Apply(edit);
            builder.SaveTo(v);
        }
        // 根据当前各个level的大小，为Version v计算执行Compaction的最佳level
        Finalize(v);

        std::string new_manifest_file;
        Status s;
        // 2. 如果MANIFEST文件指针不存在，便创建并初始化一个新的MANIFEST文件。
        // 这只会发生在第一次打开一个数据库时，MANIFEST文件在第一次创建时会写入
        // 一个current version作为快照。
        if(descriptor_log_ == nullptr) {
            assert(descriptor_file_ == nullptr);
            // 创建MANIFEST文件
            new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_); // 获取文件名
            edit->SetNextFile(next_file_number_);
            s = env_->NewWritableFile(new_manifest_file, &descriptor_file_); // 创建文件
            if(s.ok()) {
                // 创建manifest文件的Writer，可以看出manifest是用写日志的方式写入
                // 数据。
                descriptor_log_ = new log::Writer(descriptor_file_);
                // 将当前Version作为快照写入manifest文件，这只发生在第一次创建
                // 并打开manifest文件时。
                s = WriteSnapshot(descriptor_log_);
            }
        }

        // 3. 将VersionEdit写入manifest文件，此外，在执行昂贵的 MANIFEST log write 时先 Unlock
        {
            mu->Unlock();

            // 向MANIFEST log中写入新的record
            if(s.ok()) {
                // 将VersionEdit序列化为一个record，并写入manifest文件
                std::string record;
                edit->EncodeTo(&record);
                s = descriptor_log_->AddRecord(record);
                if(s.ok()) {
                    s = descriptor_file_->Sync();
                }
                if(!s.ok()) {
                    Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
                }
            }

            // 如果刚才创建了一个MANIFEST文件，则将其设置为CURRENT文件
            if(s.ok() && !new_manifest_file.empty()) {
                s = SetCurrentFile(env_, dbname_, manifest_file_number_);
            }

            mu->Lock();
        }

        // 4. 将新的Version添加到VersionSet
        if(s.ok()) {
            AppendVersion(v);
            log_number_ = edit->log_number_;
            prev_log_number_ = edit->prev_log_number_;
        } else {
            delete v;
            if(!new_manifest_file.empty()) {
                delete descriptor_log_;
                delete descriptor_file_;
                descriptor_log_ = nullptr;
                descriptor_file_ = nullptr;
                env_->RemoveFile(new_manifest_file);
            }
        }

        return s;
    }

    // 根据MANIFEST文件恢复Version。
    // CURRENT文件中存了当前的MANIFEST文件名，MANIFEST文件中存的是以log形式
    // 写入的VersionEdit（此外还包括某个版本的Version，也即一个快照）。
    Status VersionSet::Recover(bool* save_manifest) {

        struct LogReporter : public log::Reader::Reporter {
            Status* status;
            void Corruption(size_t bytes, const Status& s) override {
                if(this->status->ok()) {
                    *this->status = s;
                }
            }
        };

        // 读取CURRENT文件，CURRENT文件记录了当前的MANIFEST文件名(MANIFEST文件存储的是VersionEdit)
        // 读取结果存到current。
        std::string current;
        Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
        if(!s.ok()) {
            return s;
        }

        if(current.empty() || current[current.size() - 1] != '\n') {
            return Status::Corruption("CURRENT file does not end with newline");
        }
        current.resize(current.size() - 1);
        // 构造完整的MANIFEST文件名
        std::string dscname = dbname_ + "/" + current;
        // 构造MANIFEST文件的读取对象
        SequentialFile* file;
        s = env_->NewSequentialFile(dscname, &file);
        if(!s.ok()) {
            if(s.IsNotFound()) {
                return Status::Corruption("CURRENT points to a non-existent file", s.ToString());
            }
            return s;
        }

        bool have_log_number = false;
        bool have_prev_log_number = false;
        bool have_next_file = false;
        bool have_last_sequence = false;
        uint64_t next_file = 0;
        uint64_t last_sequence = 0;
        uint64_t log_number = 0;
        uint64_t prev_log_number = 0;
        Builder builder(this, current_);
        // 记录读取到的record的数量
        int read_records = 0;
        // 开始读取
        {
            LogReporter reporter;
            reporter.status = &s;
            // 构造MANIFEST文件的Reader
            log::Reader reader(file, &reporter, true, 0);
            Slice record;
            std::string scratch;
            // 开始读取MANIFEST文件，每次将读到的结构存到scratch，然后根据scratch构造Slice record
            while(reader.ReadRecord(&record, &scratch) && s.ok()) {
                ++read_records;
                // 读取一条VersionEdit record, MANIFEST是以log形式追加写入的VersionEdit文件
                VersionEdit edit;
                s = edit.DecodeFrom(record);
                if(s.ok()) {
                    if(edit.has_comparator_ && edit.comparator_ != icmp_.user_comparator()->Name()) {
                        s = Status::InvalidArgument(
                                edit.comparator_ + " does not match existing comparator ",
                                icmp_.user_comparator()->Name()
                                );
                    }
                }
                // 读取此VersionEdit到builder的状态中
                if(s.ok()) {
                    builder.Apply(&edit);
                }

                if (edit.has_log_number_) {
                    log_number = edit.log_number_;
                    have_log_number = true;
                }

                if (edit.has_prev_log_number_) {
                    prev_log_number = edit.prev_log_number_;
                    have_prev_log_number = true;
                }

                if (edit.has_next_file_number_) {
                    next_file = edit.next_file_number_;
                    have_next_file = true;
                }

                if (edit.has_last_sequence_) {
                    last_sequence = edit.last_sequence_;
                    have_last_sequence = true;
                }
            }
        }
        delete file;
        file = nullptr;

        if(s.ok()) {
            if(!have_next_file) {
                s = Status::Corruption("no meta-nextfile entry in descriptor");
            } else if (!have_log_number) {
                s = Status::Corruption("no meta-lognumber entry in descriptor");
            } else if (!have_last_sequence) {
                s = Status::Corruption("no last-sequence-number entry in descriptor");
            }

            if(!have_prev_log_number) {
                prev_log_number = 0;
            }

            MarkFileNumberUsed(prev_log_number);
            MarkFileNumberUsed(log_number);
        }

        if(s.ok()) {
            Version* v = new Version(this);
            // 将从MANIFEST文件读取到的VersionEdit全部应用到这个新的Version v中
            builder.SaveTo(v);
            // 将此Version添加到VersionSet作为CURRENT
            Finalize(v);
            AppendVersion(v);
            manifest_file_number_ = next_file;
            next_file_number_ = next_file + 1;
            last_sequence_ = last_sequence_;
            log_number_ = log_number;
            prev_log_number_ = prev_log_number;

            // 查看能否复用现有的MANIFEST文件
            if(ReuseManifest(dscname, current)) {
                // No need to save new manifest
            } else {
                *save_manifest = true;
            }

        } else {
            std::string error = s.ToString();
            Log(options_->info_log, "Error recovering version set with %d records: %s",
                read_records, error.c_str());
        }

        return s;
    }

    bool VersionSet::ReuseManifest(const std::string &dscname, const std::string &dscbase) {
        if(!options_->reuse_logs) {
            return false;
        }

        FileType manifest_type;
        uint64_t manifest_number;
        uint64_t manifest_size;
        // 解析文件
        if(!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
            manifest_type != kDescriptorFile ||
            !env_->GetFileSize(dscname, &manifest_size).ok() ||
            manifest_size >= TargetFileSize(options_) ) {

            return false;
        }

        assert(descriptor_file_ == nullptr);
        assert(descriptor_log_ == nullptr);
        Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
        if(!r.ok()) {
            Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
            assert(descriptor_file_ == nullptr);
            return false;
        }

        Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
        descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
        manifest_number = manifest_number;
        return true;
    }

    // 标记number已经被用掉了，将next_file_number赋值为number + 1
    void VersionSet::MarkFileNumberUsed(uint64_t number) {
        if(next_file_number_ <= number) {
            next_file_number_ = number + 1;
        }
    }

    // 根据各个level的当前大小和其容量上限，计算获取指定Version的最迫切需要Compaction的level
    void VersionSet::Finalize(Version *v) {
        // 计算下一次执行compaction的最佳level
        int best_level = -1;
        double best_score = -1;
        // 找到得分最高的level，得分最高的便是最佳compaction level
        for(int level = 0; level < config::kNumLevels - 1; level++) {
            double score;
            // 对于level-0，leveldb选择限制文件数量而不是字节数量，原因如下：
            // 1. 当写缓冲区较大时，level0最好不要过于频繁执行compaction；
            // 2. level-0中的文件在每次读取时都会合并，因此希望在单个文件较小时避免
            //    文件过多（写缓冲区可能会设置的比较小，或者压缩率比较高，或者有很多的
            //    覆盖写/删除操作）。
            if(level == 0) {
                score = v->files_[level].size() /
                        static_cast<double>(config::kL0_CompactionTrigger);
            } else {
                // 计算(current size) / (size limit)
                const uint64_t level_bytes = TotalFileSize(v->files_[level]);
                score = static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
            }

            if( score > best_score) {
                best_level = level;
                best_score = score;
            }
        }

        v->compaction_level_ = best_level;
        v->compaction_score_ = best_score;
    }

    // 将当前VersionSet的current_指针指向的Version作为快照写到磁盘的MANIFEST文件
    Status VersionSet::WriteSnapshot(log::Writer *log) {
        // Version的相关数据作为转存到VersionEdit，VersionEdit作为
        // 实际的快照写入磁盘MANIFEST文件
        VersionEdit edit;
        // 存储metadata
        edit.SetComparatorName(icmp_.user_comparator()->Name());

        // 存储compaction pointers
        for(int level = 0; level < config::kNumLevels; level++) {
            if(!compact_pointer_[level].empty()) {
                InternalKey key;
                key.DecodeFrom(compact_pointer_[level]);
                edit.SetCompactPointer(level, key);
            }
        }

        // 存储files
        for(int level =0; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData*>& files = current_->files_[level];
            for(size_t i = 0; i < files.size(); i++) {
                const FileMetaData* f = files[i];
                edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
            }
        }

        // 将VersionEdit序列化为一个字符串
        std::string record;
        edit.EncodeTo(&record);
        // 写到磁盘
        return log->AddRecord(record);
    }

    // 获取当前Version在指定level的文件数量
    int VersionSet::NumLevelFiles(int level) const {
        assert(level >= 0);
        assert(level < config::kNumLevels);
        return current_->files_[level].size();
    }

    // 汇总各个level的文件数量并返回
    const char* VersionSet::LevelSummary(LevelSummaryStorage *scratch) const {
        static_assert(config::kNumLevels == 7, "");
        std::snprintf(
                scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
                int(current_->files_[0].size()), int(current_->files_[1].size()),
                int(current_->files_[2].size()), int(current_->files_[3].size()),
                int(current_->files_[4].size()), int(current_->files_[5].size()),
                int(current_->files_[6].size())
        );
        return scratch->buffer;
    }

    // 估计ikey在Version v中的位置，单位为字节
    uint64_t VersionSet::ApproximateOffsetOf(Version *v, const InternalKey &ikey) {
        // 用于记录偏移量
        uint64_t result = 0;
        for(int level = 0; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData*>& files = v->files_[level];
            for(size_t i = 0; i < files.size(); i++) {
                if(icmp_.Compare(files[i]->largest, ikey) <= 0) {
                    // file->largest <= ikey, 说明key range能覆盖ikey的file还在后面
                    result += files[i]->file_size;
                } else if(icmp_.Compare(files[i]->smallest, ikey) > 0) {
                    if(level > 0) {
                        // level-0以下的level是按照file->smallest排序的，这里
                        // smallest都大于ikey，说明该level不存在key range覆盖此key的file，
                        // 继续查找下一层
                        break;
                    }
                } else {
                    // 当前file的key range覆盖了ikey，构造此table file的迭代器进一步估计file内部的偏移量
                    Table* tableptr;
                    Iterator* iter = table_cache_->NewIterator(
                            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr );
                    if(tableptr != nullptr) {
                        // file offset + infile offset
                        result += tableptr->ApproximateOffset(ikey.Encode());
                    }
                    delete iter;
                }
            }
        }

        return result;
    }

    // 将所有live file的编号添加到*live
    void VersionSet::AddLiveFiles(std::set<uint64_t> *live) {
        for(Version* v = dummy_versions_.next_; v != &dummy_versions_; v = v->next_) {
            for(int level = 0; level < config::kNumLevels; level++) {
                const std::vector<FileMetaData*>& files = v->files_[level];
                for(size_t i = 0; i < files.size(); i++) {
                    live->insert(files[i]->number);
                }
            }
        }
    }

    // 获取指定level的字节数
    int64_t VersionSet::NumLevelBytes(int level) const {
        assert(level >= 0);
        assert(level < config::kNumLevels);
        return TotalFileSize(current_->files_[level]);
    }

    // 获取与next level的最大重叠文件数
    int64_t VersionSet::MaxNextLevelOverlappingBytes() {
        int64_t result = 0; // 记录最大重叠文件数量
        std::vector<FileMetaData*> overlaps;
        for(int level = 1; level < config::kNumLevels; level++) {
            for(size_t i = 0; i < current_->files_[level].size(); i++) {
                const FileMetaData* f = current_->files_[level][i];
                // 计算与next level的重叠文件数量
                current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                                               &overlaps);
                const int64_t sum = TotalFileSize(overlaps);
                // 更新最大重叠文件数量
                if(sum > result) {
                    result = sum;
                }
            }
        }
        return result;
    }

    // 获取inputs中所有file的最大键和最小键，将结果保存在smallest 和 largest中
    void VersionSet::GetRange(const std::vector<FileMetaData *> &inputs,
                              InternalKey *smallest, InternalKey *largest) {
        assert(!inputs.empty());
        smallest->Clear();
        largest->Clear();
        // 遍历所有file，找到最大key和最小key
        for(size_t i = 0; i < inputs.size(); i++) {
            FileMetaData* f = inputs[i];
            if( i == 0 ) {
                *smallest = f->smallest;
                *largest = f->largest;
            } else {
                if(icmp_.Compare(f->smallest, *smallest) < 0) {
                    *smallest = f->smallest;
                }
                if(icmp_.Compare(f->largest, *largest) > 0) {
                    *largest = f->largest;
                }
            }
        }
    }

    // 获取inputs1和inputs2中所有entries的最大key和最小key，将结果存在*smallest
    // 和*largest.
    void VersionSet::GetRange2(const std::vector<FileMetaData *> &inputs1,
                               const std::vector<FileMetaData *> &inputs2,
                               InternalKey *smallest, InternalKey *largest) {
        // 合并inputs1和inputs2
        std::vector<FileMetaData*> all = inputs1;
        all.insert(all.end(), inputs2.begin(), inputs2.end());
        // 找出最大和最小key
        GetRange(all, smallest, largest);
    }


    // 对要执行Compaction操作的file构造迭代器
    Iterator* VersionSet::MakeInputIterator(Compaction *c) {
        ReadOptions options;
        options.verify_checksums = options_->paranoid_checks;
        options.fill_cache = false;

        // compaction 可以分为两种情况 ：
        // 1. level-0 和 level-1 执行compact，因为level-0的不同file存在重叠，所以要对其
        //    level-0的文件进行合并。
        // 2. level-i 和 leve-i+1 执行compact，其中i>=1
        // 注：c->inputs 是要执行compaction的两个level的file

        // 执行compaction的level为level-0时，要根据level-0的文件数量分配空间
        const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
        Iterator** list = new Iterator*[space];
        int num = 0;
        for(int which = 0; which < 2; which++) {
            if(!c->inputs_[which].empty()) {
                // 执行compact的是level-0, 当前输入也是level-0的文件.
                // 因为level-0不是严格有序的，文件的key range可能有重叠，所以
                // 不能使用NewTwoLevelIterator构造双层迭代器来读取数据。
                if(c->level() + which == 0) {
                    const std::vector<FileMetaData*>& files = c->inputs_[which];
                    // 不能通过构造双层迭代器读取数据，每个file都要构造一个迭代器
                    for(size_t i = 0; i < files.size(); i++) {
                        list[num++] = table_cache_->NewIterator(options, files[i]->number,
                                                                files[i]->file_size);
                    }

                } else {
                    // 其他level的输入，除了level-0，其他level都严格有序，可以使用双层
                    // 迭代器来读取数据。
                    list[num++] = NewTwoLevelIterator(
                            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
                            &GetFileIterator, table_cache_, options
                            );

                }
            }
        }

        assert(num <= space);
        Iterator* result = NewMergingIterator(&icmp_, list, num);
        delete[] list;
        return result;
    }

    // 根据要执行compact的level选择具体的文件，然后构造成Compaction对象并返回
    Compaction* VersionSet::PickCompaction() {
        Compaction* c;
        int level;

        // 触发compaction的两种情况：
        //  1. size compaction：一个level中的数据超过阈值；
        //  2. seek compaction：一个level中某个文件的无效查询次数过多，例如：要查询某个key，但是在查询
        //     该到key之前总会额外查询某个文件，造成非必要查询。
        // 而且leveldb更偏爱由大小超限所引起的压缩、

        const bool size_compaction = (current_->compaction_score_ >= 1); // 大小超限引起压缩
        const bool seek_compaction = (current_->file_to_compact_ != nullptr); // 无效查询引起压缩

        // size compaction的优先级更高
        if(size_compaction) {
            // 要执行compact的level
            level = current_->compaction_level_;
            assert(level >= 0);
            assert(level + 1 < config::kNumLevels);
            // 构造Compaction对象，其中包含了要执行compact的level和相关文件
            c = new Compaction(options_, level);

            // 选择level的压缩点后的第一个文件, 此文件便是具体要执行compact的文件，
            // 将其存入Compaction对象
            for(size_t i = 0; i < current_->files_[level].size(); i++) {
                FileMetaData* f = current_->files_[level][i];
                if(compact_pointer_[level].empty() ||
                   icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) >= 0) {
                    // 找到要执行compaction的文件后将其存入Compaction对象
                    c->inputs_[0].push_back(f);
                    break;
                }
            }
            // 没有压缩点，则选择level的第一个文件
            if(c->inputs_[0].empty()) {
                c->inputs_[0].push_back(current_->files_[level][0]);
            }

        } else if(seek_compaction) {
            // 若是无效查询过多引起的压缩，则直接将该文件存入Compaction对象即可
            level = current_->file_to_compact_level_;
            c = new Compaction(options_, level);
            c->inputs_[0].push_back(current_->file_to_compact_);
        } else {
            return nullptr;
        }

        c->input_version_ = current_;
        c->input_version_->Ref();

        // level-0中的文件之间可能会有重叠，所以若执行compaction的level为level-0，
        // 则需要检查是否同层还有重叠的文件，将其也加入Compaction对象.
        if(level == 0) {
            InternalKey smallest, largest;
            GetRange(c->inputs_[0], &smallest, &largest);
            current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
            assert(!c->inputs_[0].empty());
        }

        // 根据选择好的要执行压缩的第一个文件，匹配其他需要一块执行compaction的文件
        SetupOtherInputs(c);

        return c;
    }

    // 查找files中的最大key，将其存在*largest_key中，若files不为空
    // 的话则返回true。
    bool FindLargestKey(const InternalKeyComparator& icmp,
                        const std::vector<FileMetaData*>& files,
                        InternalKey* largest_key) {
        if(files.empty()) {
            return false;
        }
        *largest_key = files[0]->largest;
        for(size_t i = 1; i < files.size(); ++i) {
            FileMetaData* f = files[i];
            if(icmp.Compare(f->largest, *largest_key) > 0) {
                *largest_key = f->largest;
            }
        }
        return true;
    }

    // 在level_files中找到满足
    //  1. file.smallest > largest 且
    //  2. user_key(file.smallest) == user_key(largest)
    // 的最小边界file。
    FileMetaData* FindSmallestBoundaryFile(
            const InternalKeyComparator& icmp,
            const std::vector<FileMetaData*>& level_files,
            const InternalKey& largest_key) {

        const Comparator* user_cmp = icmp.user_comparator();
        FileMetaData* smallest_boundary_file = nullptr;
        for(size_t i = 0; i < level_files.size(); ++i) {
            FileMetaData* f = level_files[i];
            // 找到f->smallest > largest_key && user_key(f->smallest) = user_key(largest）的
            // boundary file
            if(icmp.Compare(f->smallest, largest_key) > 0 &&
               user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) == 0) {
                // 找到最小的boundary file
                if(smallest_boundary_file == nullptr ||
                   icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
                    smallest_boundary_file = f;
                }
            }
        }

        return smallest_boundary_file;
    }

    // 根据compaction_files的largest_key在level_files中查找boundary file，并
    // 将其添加到compaction_files中作为compact的输入。
    void AddBoundaryInputs(const InternalKeyComparator& icmp,
                           const std::vector<FileMetaData*>& level_files,
                           std::vector<FileMetaData*>* compaction_files) {

        // 在compaction_files中找到largest_key
        InternalKey largest_key;
        if(!FindLargestKey(icmp, *compaction_files, &largest_key)) {
            return;
        }

        bool continue_searching = true;
        while(continue_searching) {
            // 在level_files中找到boundary file
            FileMetaData* smallest_boundary_file =
                    FindSmallestBoundaryFile(icmp, level_files, largest_key);
            // 继续找boundary file的boundary file，直到没有最后找到的boundary file与其临界文件没有交际
            if(smallest_boundary_file != nullptr) {
                compaction_files->push_back(smallest_boundary_file);
                largest_key = smallest_boundary_file->largest;
            } else {
                continue_searching = false;
            }
        }
    }

    // 设置Compaction对象执行compact操作所需要的其他输入文件
    void VersionSet::SetupOtherInputs(Compaction *c) {
        const int level = c->level();
        InternalKey smallest, largest;

        // 查找起始输入文件c->inputs[0]在level中的边界文件，并将其加入到c->inputs[0]
        AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
        GetRange(c->inputs_[0], &smallest, &largest);

        // 查找level + 1中与level的key range有重叠的文件，存到c->inputs_[1]，得到起始输入文件c->inputs[1]
        current_->GetOverlappingInputs(level + 1, &smallest, &largest, &c->inputs_[1]);
        // 查找起始输入文件c->inputs[1]在level+1中的边界文件，并将其加入到c->inputs[1]
        AddBoundaryInputs(icmp_, current_->files_[level + 1], &c->inputs_[1]);

        // 获取compaction涉及到的所有文件的最大和最小key
        InternalKey all_start, all_limit;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

        if(!c->inputs_[1].empty()) {
            // 根据all_start和all_limit，在level中查找存在重叠的文件，然后再查找boundary file，
            // 完成对c->inputs[0]的扩充，得到expanded0.
            std::vector<FileMetaData*> expanded0;
            current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
            AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);

            const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
            const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
            const int64_t expanded0_size = TotalFileSize(expanded0);

            if(expanded0.size() > c->inputs_[0].size() &&
               inputs1_size + expanded0_size < ExpandedCompactionByteSizeLimit(options_)) {
                // 根据对c->inputs_[0]扩充后的expanded0, 完成对c->inputs[1]的扩充，
                // 得到expanded1.
                InternalKey new_start, new_limit;
                GetRange(expanded0, &new_start, &new_limit);
                std::vector<FileMetaData*> expanded1;
                current_->GetOverlappingInputs(level + 1, &new_start, &new_limit, &expanded1);
                AddBoundaryInputs(icmp_, current_->files_[level+1], &expanded1);

                if(expanded1.size() == c->inputs_[1].size()) {
                    Log(options_->info_log,
                        "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
                        level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
                        long(inputs0_size), long(inputs1_size), int(expanded0_size),
                        int(expanded1.size()), long(expanded0_size), long(inputs1_size) );

                    smallest = new_start;
                    largest = new_limit;
                    c->inputs_[0] = expanded0;
                    c->inputs_[1] = expanded1;
                    GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
                }
            }
        }

        // 获取与此次compaction有重叠的grandparent files。
        // （parent = level + 1， grandparent = level + 2）
        if(level + 2 < config::kNumLevels) {
            current_->GetOverlappingInputs(level + 2, &all_start, &all_limit, &c->grandparents_);
        }
        // 本次compact的最大key即为level的压缩点
        compact_pointer_[level] = largest.Encode().ToString();
        c->edit_.SetCompactPointer(level, largest);
    }

    // 构造并返回一个在指定level上对范围[begin, end]执行compaction的compaction对象。
    // 也即根据compaction range 构造Compaction对象。
    Compaction* VersionSet::CompactRange(int level, const InternalKey *begin, const InternalKey *end) {
        // 根据键值范围，获取与该范围有重叠的文件，这些重叠文件将作为compact的输入
        std::vector<FileMetaData*> inputs;
        current_->GetOverlappingInputs(level, begin, end, &inputs);
        if(inputs.empty()) {
            return nullptr;
        }

        if(level > 0) {
            const uint64_t limit = MaxFileSizeForLevel(options_, level);
            uint64_t total = 0;
            for(size_t i = 0; i < inputs.size(); i++) {
                uint64_t s = inputs[i]->file_size;
                total += s;
                if(total >= limit) {
                    inputs.resize(i + 1);
                    break;
                }
            }
        }

        Compaction* c = new Compaction(options_, level);
        c->input_version_ = current_;
        c->input_version_->Ref();
        c->inputs_[0] = inputs;
        SetupOtherInputs(c);
        return c;
    }

    Compaction::Compaction(const Options* options, int level)
        : level_(level),
          max_output_file_size_(MaxFileSizeForLevel(options, level)),
          input_version_(nullptr),
          grandparent_index_(0),
          seen_key_(false),
          overlapped_bytes_(0) {

        for(int i = 0; i < config::kNumLevels; i++) {
            level_ptrs_[i] = 0;
        }
    }

    Compaction::~Compaction() {
        if(input_version_ != nullptr) {
            input_version_->Unref();
        }
    }

    // 是否可以仅仅将SSTable移动到下层就可以完成compaction操作
    bool Compaction::IsTrivialMove() const {
        const VersionSet* vset = input_version_->vset_;
        // 如果可以仅仅通过移动上层level的file到下层level便可以完成compaction，且
        // compact结果与grandparent没有太多重叠，则返回true。
        // 与grandparent有太多重叠的话后续会有非常高的合并成本。
        return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
                TotalFileSize(grandparents_) <= MaxGrandParentOverlapBytes(vset->options_) );
    }

    // 在VersionEdit中记录完成compaction操作后需要删除的文件，也即compaction中的输入文件
    void Compaction::AddInputDeletions(VersionEdit *edit) {
        for(int which = 0; which < 2; which++) {
            for(size_t i = 0; i < inputs_[which].size(); i++) {
                edit->RemoveFile(level_ + which, inputs_[which][i]->number);
            }
        }
    }

    bool Compaction::IsBaseLevelForKey(const Slice &user_key) {
        const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
        for(int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
            const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
            while(level_ptrs_[lvl] < files.size()) {
                FileMetaData* f = files[level_ptrs_[lvl]];
                if(user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
                    // 找到相同的key则返回false
                    if(user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
                        return false;
                    }
                    break;
                }
                level_ptrs_[lvl]++;
            }
        }
        return true;
    }

    bool Compaction::ShouldStopBefore(const Slice &internal_key) {
        const VersionSet* vset = input_version_->vset_;
        const InternalKeyComparator* icmp = &vset->icmp_;
        while (grandparent_index_ < grandparents_.size() &&
               icmp->Compare(internal_key,
                             grandparents_[grandparent_index_]->largest.Encode()) > 0) {
            if(seen_key_) {
                overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
            }
            grandparent_index_++;
        }
        seen_key_ = true;
        if(overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
            overlapped_bytes_ = 0;
            return true;
        } else {
            return false;
        }
    }

    void Compaction::ReleaseInputs() {
        if(input_version_ != nullptr) {
            input_version_->Unref();
            input_version_ = nullptr;
        }
    }


} // end namespace leveldb


