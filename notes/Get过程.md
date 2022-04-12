## 0x00

### 查询操作的入口函数:

`Status DBImpl::Get(const ReadOptions &options, const Slice &key, std::string *value)`

- 函数声明所在文件`db/db_impl.h`
- 函数实现所在文件`db/db_impl.cc`

代码：

```c++
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
```

可以看到当未在Memtable和Immutable Memtable中查找到数据时，会调用:

```C++
s = current->Get(options, lkey, value, &stats);
```

来进一步查找。VersionSet类中的current指针指向数据库最新版本的Version。下面进一步查看Version中的`Get`函数。

- 函数声明所在文件db/version_set.h
- 函数定义所在文件db/version_set.cc

代码：

```c++
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
                        // 继续在当前文件查找
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

        // 找key range覆盖指定key的文件，也即找到可能包含此key的文件，然后调用Match方法进一步查找
        ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

        return state.found ? state.s : Status::NotFound(Slice());
}
```

可以看到，该函数调用了函数：

```C++
ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);
```

而且还传入了一个回调函数`State::Match`。先看`State::Match`函数的作用，该函数通过调用

```C++
Status TableCache::Get(const ReadOptions &options, uint64_t file_number, uint64_t file_size, const Slice &k,
                           void *arg, void (*handle_result)(void *, const Slice &, const Slice &)) {

        Cache::Handle* handle = nullptr;
        // 找table
        Status s = FindTable(file_number, file_size, &handle);
        // 找到table（找到后会加载到缓存）
        if(s.ok()) {
            Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
            // 根据key查找，查找到后调用handle_result函数
            s = t->InternalGet(options, k, arg, handle_result);
            // 找到table后就可以释放缓存节点了
            cache_->Release(handle);
        }

        return s;
}
```

在SSTable中查找指定key。

下面再来看一下`ForEachOverlapping`函数。

```c++
void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void *arg,
                                     bool (*func)(void *, int, FileMetaData *)) ;
```

- `ForEachOverlapping`函数声明所在文件`db/version_set.h`
- `ForEachOverlapping`函数定义所在文件`db/version_set.cc`

函数代码：

```c++
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
```



