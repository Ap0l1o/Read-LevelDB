//
// Created by Ap0l1o on 2022/3/26.
//

#include "leveldb/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {
    Cache::~Cache() {}

    namespace {

        // LRU cache implementation  LRU cache 的实现
        //
        // 每个cache entry都有一个名为「in_cache」的布尔变量，用来指示cache中是否有该entry的引用
        // 在没有将该entry传递给「deleter」之前，「in_cache」变为false的唯一方式是通过Erase()，或者通过
        // Insert()插入重复key时，或者cache被破坏时；
        //
        // Cache在缓存中保留了两个链表。Cache中的所有item存在且只存在于这两个链表中的一个，
        // 从Cache中删除但仍被客户端引用的item不在这两个链表中。
        // 这两个链表为：
        // - in-use: 包含客户端当前引用的item，没有特定的顺序（此列表用于不变检查，如果我们删除检查
        //            ，则该列表中的元素可能会保留为断开连接的单例链表）。
        // - LRU: 包含客户端当前未引用的item，按LRU排序。
        //
        // 当Ref()和Unref()方法检测到一个元素获得或丢失掉其唯一的外部引用时，元素会在这两个链表之间移动，。

        // entry是可变长度的堆分配的结构。entry保存在按访问时间排序的循环双向链表中。
        struct LRUHandle {
            void* value;
            void (*deleter)(const Slice&, void* value);
            LRUHandle* next_hash;
            LRUHandle* next;
            LRUHandle* prev;
            size_t charge;
            size_t key_length;
            // entry是否在缓存中
            bool in_cache;
            // 引用数量，包括缓存的引用
            uint32_t refs;
            // key的哈希值，用于快速分片和比较
            uint32_t hash;
            // key的开头
            char key_data[1];

            Slice key() const {
                assert(next != this);
                return Slice(key_data, key_length);
            }

        };

        class HandleTable {
        public:
            HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
            ~HandleTable() { delete[] list_; }

            // 从缓存中查找匹配项
            LRUHandle* Lookup(const Slice& key, uint32_t hash) {
                return *FindPointer(key, hash);
            }

            // 向缓存链表中插入一个缓存项
            LRUHandle* Insert(LRUHandle* h) {

                // 具体过程为先找到bucket，再在bucket中找插入位置，
                // 若bucket中已有一个与该元素key一致的缓存项，则返回该缓存项的位置，插入过程变为一个更新替换过程，
                // 缓存的元素数量没有变化；
                // 否则返回尾指针，插入在尾部，此时是真正的插入了一个元素，元素数量需要加1

                // 找插入位置
                LRUHandle** ptr = FindPointer(h->key(), h->hash);
                LRUHandle* old = *ptr;
                // 插入
                h->next_hash = (old == nullptr ? nullptr : old->next_hash);
                *ptr = h;
                // 原bucket中不存在相同key的缓存项，确实是真的插入了一个元素，
                // 缓存元素数量需要加1
                if(old == nullptr) {
                    ++elems_;
                    // 由于每个缓存条目都很大，因此这里的目标是使平均链表长度<=1
                    if(elems_ > length_) {
                        Resize();
                    }
                }
                return old;
            }

            // 从缓存中移除指定的key/hash项，并返回指向该缓存项的指针
            LRUHandle* Remove(const Slice& key, uint32_t hash) {
                // 找到key的位置
                LRUHandle** ptr = FindPointer(key, hash);
                LRUHandle* result = *ptr;
                // 如果成功找到，则从链表删除
                if(result != nullptr) {
                    *ptr = result->next_hash;
                    --elems_;
                }
                // 返回查找到的结果
                return result;
            }

        private:
            // 该table由一组bucket组成，每个bucket是一个存储entry的链表，entry通过散列的方式
            // 分配到一个bucket。
            // 也即该table是一个链表数组（数组中的每个节点也是一个链表）

            // 缓存大小（外层链表的长度，也即bucket的数量）
            uint32_t length_;
            // 当前table中缓存元素的数量
            uint32_t elems_;
            // 链表数组，list_的每个节点是一个链表
            // 一个key/hash，会根据hash来散列到list_的一个链表中存储
            LRUHandle** list_;

            // 返回指向匹配key/hash的缓存项指针，如果找不到匹配项
            // 则返回尾指针
            LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
                // 根据hash找存储该key的子链表，也即根据hash找到bucket
                LRUHandle** ptr = &list_[hash & (length_ - 1)];
                // 在bucket中找到指向key的指针
                while(*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
                    ptr = &(*ptr)->next_hash;
                }
                return ptr;
            }

            void Resize() {
                uint32_t new_length = 4;
                while(new_length < elems_) {
                    new_length *= 2;
                }
                LRUHandle** new_list = new LRUHandle*[new_length];
                memset(new_list, 0, sizeof(new_list[0]) * new_length);
                uint32_t count = 0;
                for(uint32_t i = 0; i < length_; i++) {
                    LRUHandle* h = list_[i];
                    while(h != nullptr) {
                        LRUHandle* next = h->next_hash;
                        uint32_t hash = h->hash;
                        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
                        h->next_hash = *ptr;
                        *ptr = h;
                        h = next;
                        count++;
                    }
                }
                assert(elems_ == count);
                delete[] list_;
                list_ = new_list;
                length_ = new_length;
            }
        };

        // 分片缓存的单个分片
        class LRUCache {
        public:
            LRUCache();
            ~LRUCache();

            void SetCapacity(size_t capacity) { capacity_ = capacity; }

            Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                                  size_t charge,
                                  void (*deleter)(const Slice& key, void* value));

            Cache::Handle* Lookup(const Slice& key, uint32_t hash);

            void Release(Cache::Handle* handle);
            void Erase(const Slice& key, uint32_t hash);
            void Prune();
            size_t TotalCharge() const {
                MutexLock l(&mutex_);
                return usage_;
            }

        private:
            void LRU_Remove(LRUHandle* e);
            void LRU_Append(LRUHandle* list, LRUHandle* e);
            void Ref(LRUHandle* e);
            void Unref(LRUHandle* e);
            bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

            size_t capacity_;
            mutable port::Mutex mutex_;
            // 以下变量使用线程安全注解，GUARDED_BY(mutex_) 表示这些成员变量
            // 受mutex_保护
            size_t usage_ GUARDED_BY(mutex_);
            LRUHandle lru_ GUARDED_BY(mutex_);
            LRUHandle in_use_ GUARDED_BY(mutex_);
            HandleTable table_ GUARDED_BY(mutex_);
        };

        LRUCache::LRUCache() : capacity_(0), usage_(0) {
            // 创建空的循环链表
            lru_.next = &lru_;
            lru_.prev = &lru_;
            in_use_.next = &in_use_;
            in_use_.prev = &in_use_;
        }

        LRUCache:: ~LRUCache() {
            assert(in_use_.next == &in_use_);
            for(LRUHandle* e = lru_.next; e != &lru_; ) {
                LRUHandle* next = e->next;
                assert(e->in_cache);
                e->in_cache = false;
                assert(e->refs == 1);
                Unref(e);
                e = next;
            }
        }

        void LRUCache::Ref(LRUHandle *e) {
            // 如果在lru_链表中，则将其移到in_use_链表中
            if(e->refs == 1 && e->in_cache) {
                LRU_Remove(e);
                LRU_Append(&in_use_, e);
            }
            e->refs++;
        }

        void LRUCache::Unref(LRUHandle *e) {
            assert(e->refs > 0);
            e->refs--;
            if(e->refs == 0) {
                assert(!e->in_cache);
                (*e->deleter)(e->key(), e->value);
                free(e);
            } else if(e->in_cache && e->refs == 1) {
                LRU_Remove(e);
                LRU_Append(&lru_, e);
            }
        }

        void LRUCache::LRU_Remove(LRUHandle *e) {
            e->next->prev = e->prev;
            e->prev->next = e->next;
        }

        void LRUCache::LRU_Append(LRUHandle *list, LRUHandle *e) {
            // 将e作为新的entry，插在*list之前
            e->next = list;
            e->prev = list->prev;
            e->prev->next = e;
            e->next->prev = e;
        }

        Cache::Handle* LRUCache::Lookup(const Slice &key, uint32_t hash) {
            MutexLock l(&mutex_);
            LRUHandle* e = table_.Lookup(key, hash);
            if(e != nullptr) {
                Ref(e);
            }
            return reinterpret_cast<Cache::Handle*>(e);
        }

        void LRUCache::Release(Cache::Handle *handle) {
            MutexLock l(&mutex_);
            Unref(reinterpret_cast<LRUHandle*>(handle));
        }

        Cache::Handle* LRUCache::Insert(const Slice &key, uint32_t hash, void *value, size_t charge,
                                        void (*deleter)(const Slice &, void *)) {
            MutexLock l(&mutex_);
            // 根据参数创建一个LRUHandle
            LRUHandle* e = reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
            e->value = value;
            e->deleter = deleter;
            e->charge = charge;
            e->key_length = key.size();
            e->hash = hash;
            e->in_cache = false;
            e->refs = 1;
            std::memcpy(e->key_data, key.data(), key.size());

            if(capacity_ > 0) {
                e->refs++;
                e->in_cache = true;
                LRU_Append(&in_use_, e);
                usage_ += charge;
                FinishErase(table_.Insert(e));
            } else {
                // 不需要缓存，capacity_==0表示不支持并关掉了缓存
                e->next = nullptr;
            }
            while(usage_ > capacity_ && lru_.next != &lru_) {
                LRUHandle* old = lru_.next;
                assert(old->refs == 1);
                bool erased = FinishErase(table_.Remove(old->key(), old->hash));
                if(!erased) {
                    assert(erased);
                }
            }
            return reinterpret_cast<Cache::Handle*>(e);
        }

        bool LRUCache::FinishErase(LRUHandle *e) {
            // 若e == nullptr，则已经吧*e移出缓存了
            if(e != nullptr) {
                assert(e->in_cache);
                LRU_Remove(e);
                e->in_cache = false;
                usage_ -= e->charge;
                Unref(e);
            }
            return e != nullptr;
        }

        void LRUCache::Erase(const Slice &key, uint32_t hash) {
            MutexLock l(&mutex_);
            FinishErase(table_.Remove(key, hash));
        }

        void LRUCache::Prune() {
            MutexLock l(&mutex_);
            while(lru_.next != &lru_) {
                LRUHandle* e = lru_.next;
                assert(e->refs == 1);
                bool erased = FinishErase(table_.Remove(e->key(), e->hash));
                if(!erased) {
                    assert(erased);
                }
            }
        }

        static const int kNumShardBits = 4;
        static const int kNumShards = 1 << kNumShardBits;


    } // end namespace

} // end namespace leveldb