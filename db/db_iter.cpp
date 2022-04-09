//
// Created by Ap0l1o on 2022/4/6.
//

#include "db/db_iter.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {


#if 0
    static void DumpInternalIter(Iterator* iter) {
        for(iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            ParsedInternalKey k;
            if(!ParsedInternalKey(iter->key(), &k)) {
                std::fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
            } else {
                std::fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
            }
        }
    }
#endif

    namespace {

        // DB中的Memtable和sstable的存储格式均为(user key, seq, type) => user value 所表示的entry。
        // DBIter在DB中找到具有相同user key的多个entry，然后根据他们的sequence number, deletion marker以及overwrite，
        // 将它们合并为一个entry。也即把同一个user key在DB中的多条记录合并为一条。
        //
        // 注：
        //      1. iter_遍历的是数据库的每一条记录。它是以 InternalKey (user key, seq, type) 为遍历粒度的，只要 InternalKey 中
        //         任意一个组成元素不同，MergingIterator 就认为他们是不同的 kv 对。
        //      2. DBIter 是以user key为遍历粒度的，只要记录的user key相同，那么DBIter就认为他们是一条记录（不同版本），sqe越
        //         大代表该记录越新。每次迭代将跳到下一个不同user key的记录。
        //
        class DBIter : public Iterator {
        public:
            // 迭代器的移动方向：
            // 1. kForward, 向前移动，保证此时DBIter的内部迭代器刚好定位在this->key()和this->value()这条
            //    记录上。
            // 2. kReverse, 向后移动，保证此时DBIter的内部迭代器刚好定位在所有key = this->key()的entry之前，
            //    saved_key_和saved_value_保存的便是kReverse方向移动时的K/V对。
            enum Direction { kForward, kReserve };

            DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
                   uint32_t seed)
                   : db_(db),
                     user_comparator_(cmp),
                     iter_(iter),
                     sequence_(s),
                     direction_(kForward),
                     valid_(false),
                     rnd_(seed),
                     bytes_until_read_sampling_(RandomCompactionPeriod()) {}

            DBIter(const DBIter&) = delete;
            DBIter& operator=(const DBIter&) = delete;
            ~DBIter() override { delete iter_; }

            bool Valid() const override { return valid_; }
            Slice key() const override {
                assert(valid_);
                return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
            }

            Slice value() const override {
                assert(valid_);
                return (direction_ == kForward) ? iter_->value() : saved_value_;
            }

            Status status() const override {
                if(status_.ok()) {
                    return iter_->status();
                } else {
                    return status_;
                }
            }

            void Next() override;
            void Prev() override;
            void Seek(const Slice& target) override;
            void SeekToFirst() override;
            void SeekToLast() override;

        private:
            void FindNextUserEntry(bool skipping, std::string* skip);
            void FindPrevUserEntry();
            bool ParseKey(ParsedInternalKey* key);

            inline void SaveKey(const Slice& k, std::string* dst) {
                dst->assign(k.data(), k.size());
            }

            inline void ClearSavedValue() {
                // 1048576 = 1024 * 1024 = 1MB
                if(saved_value_.capacity() > 1048576) {
                    std::string empty;
                    swap(empty, saved_value_);
                } else {
                    saved_value_.clear();
                }
            }

            size_t RandomCompactionPeriod() {
                return rnd_.Uniform(2 * config::kReadBytesPeriod);
            }

            DBImpl* db_;
            // 用于比较iter_中的user key的comparator
            const Comparator* const user_comparator_;
            // 一个MergingIterator，其包括查询memtable、immutable memtable和sstable的子迭代器
            Iterator* const iter_;
            // DBIter只能访问到比sequence_小的KV对,
            // 这能方便旧版本（快照）数据库的遍历。
            SequenceNumber const sequence_;
            Status status_;
            // 当direction == kReverse时，iter_指向current key的前一个key
            // 当direction_ == kReverse时的current key
            std::string saved_key_;
            // 当direction_ == kReverse时的current value
            std::string saved_value_;
            // 当前移动方向
            Direction direction_;
            bool valid_;
            Random rnd_;
            size_t bytes_until_read_sampling_;
        };

        inline bool DBIter::ParseKey(ParsedInternalKey *ikey) {
            Slice k = iter_->key();
            size_t bytes_read = k.size() + iter_->value().size();
            while(bytes_until_read_sampling_ < bytes_read) {
                bytes_until_read_sampling_ += RandomCompactionPeriod();
                db_->RecordReadSample(k);

            }
            assert(bytes_until_read_sampling_ >= bytes_read);
            bytes_until_read_sampling_ -= bytes_read;

            if(!ParseInternalKey(k, ikey)) {
                status_ = Status::Corruption("corrupted internal key in DBIter");
                return false;
            } else {
                return true;
            }
        }

        // 向后跳过同一user key的无效数据
        void DBIter::Next() {
            assert(valid_);
            // 当前移动方向为kReserve的话，iter_指向this->key()前面的entry，
            // 所以需要前移并跳过无效数据。
            // 注意还要修改移动方向为kForward。
            // 且此时saved_key_中已经保存了需要跳过的key，也即this->key()，saved_key_将用作skip。
            if(direction_ == kReserve) {
                direction_ = kForward;
                if(!iter_->Valid()) {
                    iter_->SeekToFirst();
                } else {
                    iter_->Next();
                }
                if(!iter_->Valid()) {
                    valid_ = false;
                    saved_key_.clear();
                    return;
                }

            } else {
                // 将this->key()也即iter_->key()保存在saved_key_中，用于跳过无效数据
                SaveKey(iter_->key(), &saved_key_);
                iter_->Next();
                if(!iter_->Valid()) {
                    valid_ = false;
                    saved_key_.clear();
                    return ;
                }
            }
            // 跳过无效数据
            FindNextUserEntry(true, &saved_key_);
        }

        // 查找下一个有效的entry
        void DBIter::FindNextUserEntry(bool skipping, std::string *skip) {
            assert(iter_->Valid());
            assert(direction_ == kForward);
            do {
                ParsedInternalKey ikey;
                // 将当前iter_的internal key解析，并保证其序号小于sequence_
                if(ParseKey(&ikey) && ikey.sequence <= sequence_) {
                    // 查看数据类型
                    switch (ikey.type) {
                        case kTypeDeletion:
                            // 如果是"删除"类型，则该entry会覆盖掉后面具有相同user key的entry，
                            // 将该entry的user key保存在skip中，并将skipping设置为true，用于
                            // 跳过后面具有相同user key的entry。
                            //
                            // 因为InternalKeyComparator的排序规则是按照：user key的升序，seq的降序，类型的降序来排序的，
                            // 所以能保证相同的user key的internal key，最新版本的internal key是排在前面的，能覆盖掉后面的
                            // 旧版本数据。
                            SaveKey(ikey.user_key, skip);
                            skipping = true;
                            break;
                        case kTypeValue:
                            if(skipping && user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
                                // Entry hidden 需要跳过
                            } else {
                                // 不需要跳过，已经找到下一个可用的entry
                                valid_ = true;
                                saved_key_.clear();
                                return ;
                            }
                            break;
                    }
                }

                iter_->Next();

            } while(iter_->Valid());
        }

        // 向前跳过同一user key的无效数据
        void DBIter::Prev() {
            assert(valid_);
            if(direction_ == kForward) {
                // direction == kForward时，iter_指向当前entry，所以只需要前移到一个不同的user key，
                // 然后在调用FindPrevUserEntry找到这个不同的user key的最新版本即可。
                assert(iter_->Valid());
                SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
                while (true) {
                    iter_->Prev();
                    if(!iter_->Valid()) {
                        valid_ = false;
                        saved_key_.clear();
                        ClearSavedValue();
                        return ;
                    }
                    if(user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) < 0) {
                        break;
                    }
                }
                direction_ = kReserve;
            }
            // 如果direction == kReverse 则iter_指向的是当前entry的前一个user key的entry，
            // 此时只需要直接调用FindPrevUserEntry找到最新版本即可。

            FindPrevUserEntry();
        }

        // 从后往前遍历到当前 user_key 的最新版本
        void DBIter::FindPrevUserEntry() {
            assert(direction_ == kReserve);
            ValueType value_type = kTypeDeletion;
            if(iter_->Valid()) {
                do {
                    ParsedInternalKey ikey;
                    if(ParseKey(&ikey) && ikey.sequence <= sequence_) {
                        // 不是删除节点，且是一个新的user key，结束前移，saved_key_和saved_value_保存的便是
                        // 当前user key的最新版本（也即current key和current value），iter_指向的是前一个user key的entry。
                        if((value_type != kTypeDeletion) &&
                            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
                            break;
                        }
                        // 保存当前internal key的类型
                        value_type = ikey.type;
                        // 当前internal key的类型为kTypeDeletion，清空saved_key_和saved_value_
                        if(value_type == kTypeDeletion) {
                            saved_key_.clear();
                            ClearSavedValue();
                        } else {
                            // 普通的internal key，将其保存到saved_key_和saved_value_
                            Slice raw_value = iter_->value();
                            if(saved_value_.capacity() > raw_value.size() + 1048576) {
                                std::string empty;
                                swap(empty, saved_value_);
                            }
                            SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
                            saved_value_.assign(raw_value.data(), raw_value.size());
                        }
                    }

                    iter_->Prev();

                } while (iter_->Valid());
            }
            // 前移失败，到头了
            if(value_type == kTypeDeletion) {
                valid_ = false;
                saved_key_.clear();
                ClearSavedValue();
                direction_ = kForward;
            } else {
                valid_ = true;
            }
        }

        void DBIter::Seek(const Slice& target) {
            direction_ = kForward;
            ClearSavedValue();
            // 将一个internal key（target）封装到saved_key_
            saved_key_.clear();
            AppendInternalKey(&saved_key_,
                              ParsedInternalKey(target, sequence_, kValueTypeForSeek));
            // 跳到一个合适的位置
            iter_->Seek(saved_key_);
            // 跳过无效数据
            if(iter_->Valid()) {
                FindNextUserEntry(false, &saved_key_);
            } else {
                valid_ = false;
            }
        }

        void DBIter::SeekToFirst() {
            direction_ = kForward;
            ClearSavedValue();
            iter_->SeekToFirst();
            if(!iter_->Valid()) {
                FindNextUserEntry(false, &saved_key_);
            } else {
                valid_ = false;
            }
        }

        void DBIter::SeekToLast() {
            direction_ = kReserve;
            ClearSavedValue();
            iter_->SeekToLast();
            FindPrevUserEntry();
        }

    } // end namespace

    Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator,
                            Iterator* internal_iter, SequenceNumber sequence,
                            uint32_t seed) {
        return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
    }


} // end namespace leveldb