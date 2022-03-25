//
// Created by Ap0l1o on 2022/3/25.
//

#ifndef LLEVELDB_ITERATOR_WRAPPER_H
#define LLEVELDB_ITERATOR_WRAPPER_H

#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace leveldb {
    // 一个内部包装类，其接口类似于Iterator，它缓存底层迭代器的valid()和key()的返回结果，
    // 这可以帮助避免虚函数调用，还可以提供更好的缓存局部性。
    class IteratorWrapper {

    public:
        IteratorWrapper() : iter_(nullptr), valid_(false) {}
        explicit IteratorWrapper(Iterator* iter) : iter_(nullptr) { Set(iter); }
        ~IteratorWrapper() { delete iter_; }
        Iterator* iter() const { return iter_; }

        void Set(Iterator * iter) {
            delete iter_;
            iter_ = iter;
            if(iter_ == nullptr) {
                valid_ = false;
            } else {
                Update();
            }
        }

        bool Valid() const { return valid_; }

        Slice key() const {
            assert(Valid());
            return key_;
        }

        Slice value() const {
            assert(Valid());
            return iter_->value();
        }

        Status status() const {
            assert(iter_);
            return iter_->status();
        }

        void Next() {
            assert(iter_);
            iter_->Next();
            Update();
        }
        void Prev() {
            assert(iter_);
            iter_->Prev();
            Update();
        }

        void Seek(const Slice& k) {
            assert(iter_);
            iter_->Seek(k);
            Update();
        }

        void SeekToFirst() {
            assert(iter_);
            iter_->SeekToFirst();
            Update();
        }

        void SeekToLast() {
            assert(iter_);
            iter_->SeekToLast();
            Update();
        }

    private:
        // 更新缓存
        void Update() {
            valid_ = iter_->Valid();
            if(valid_) {
                key_ = iter_->key();
            }
        }
        Iterator* iter_;
        bool valid_;
        Slice key_;
    };
} // end namespace leveldb

#endif //LLEVELDB_ITERATOR_WRAPPER_H