//
// Created by Ap0l1o on 2022/4/4.
//

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {
    namespace {
        // 归并迭代器，包含多个子迭代器，对子迭代器的结果进行归并。
        class MergingIterator : public Iterator {
        public:
            MergingIterator(const Comparator* comparator, Iterator** children, int n)
                : comparator_(comparator),
                  children_(new IteratorWrapper[n]),
                  n_(n),
                  current_(nullptr),
                  direction_(kForward) {

                for(int i = 0; i < n; i++) {
                    children_[i].Set(children[i]);
                }
            }

            ~MergingIterator() override {
                delete[] children_;
            }

            bool Valid() const override {
                return (current_ != nullptr);
            }

            void SeekToFirst() override {
                // 所有子迭代器都SeekToFirst
                for(int i = 0; i < n_; i++) {
                    children_[i].SeekToFirst();
                }
                // 然后从所有的子迭代器中选择最小的那个
                FindSmallest();
                direction_ = kForward;
            }

            void SeekToLast() override {
                // 所有的子迭代器都SeekToLast
                for(int i = 0; i < n_; i++) {
                    children_[i].SeekToLast();
                }
                // 然后从所有子迭代器中选择结果最大的那个
                FindLargest();
                direction_ = kReverse;
            }

            void Seek(const Slice& target) override {
                for(int i = 0; i < n_; i++) {
                    children_[i].Seek(target);
                }
                FindSmallest();
                direction_ = kForward;
            }

            void Next() override {
                assert(Valid());
                // 确保所有的子迭代器的当前位置都在key()后面，然后使current_指向最小key的那个子迭代器。
                // 1. 如果移动方向为kForward的话，则除了current_所指向的最小子迭代器满足key() == current_->key(),
                //    其他的子迭代器均满足上述条件，因此只需要将current_进行Next()操作；
                // 2. 如果移动方向为kReverse，则每个子迭代器都需要进行移动定位；
                // 子迭代器都移动完成之后找出最小key的子迭代器。
                if(direction_ != kForward) {
                    for(int i = 0; i < n_; i++) {
                        IteratorWrapper* child = &children_[i];
                        if(child != current_) {
                            child->Seek(key());
                            if(child->Valid() &&
                               comparator_->Compare(key(), child->key()) == 0) {
                                child->Next();
                            }
                        }
                    }
                    direction_ = kForward;
                }
                current_->Next();
                // 子迭代器都移动完了后，找出最小key的子迭代器。
                FindSmallest();
            }

            void Prev() override {
                assert(Valid());
                // 确保所有子迭代器的当前位置都在key()的前面，然后使current_指向最大key的子迭代器。
                // 1. 如果当前移动方向为kReverse，则除了current_所指向的子迭代器外均已满足条件，
                //    此时current_->key() == key(), 只需将current_执行Prev()操作；
                // 2. 如果当前的移动方向为kForward，则每个子迭代器都需要进行移动定位；
                // 子迭代器都移动完成之后找出最大key的子迭代器。
                if(direction_ != kReverse) {
                    for(int i = 0; i < n_; i++) {
                        IteratorWrapper* child = &children_[i];
                        if(child != current_) {
                            child->Seek(key());
                            if(child->Valid()) {
                                child->Prev();
                            } else {
                                child->SeekToLast();
                            }
                        }
                    }
                    direction_ = kReverse;
                }

                current_->Prev();
                // 子迭代器都移动完了后，找出最大key的子迭代器。
                FindLargest();
            }

            Slice key() const override {
                assert(Valid());
                return current_->key();
            }

            Slice value() const override {
                assert(Valid());
                return current_->value();
            }

            Status status() const override {
                Status status;
                for(int i = 0; i < n_; i++) {
                    status = children_[i].status();
                    if(!status.ok()) {
                        break;
                    }
                }
                return status;
            }

        private:
            enum Direction { kForward, kReverse };

            // 让current_指向所有子迭代器key最小的那个子迭代器
            void FindSmallest();
            // 让current_指向所有子迭代器key最大的那个子迭代器
            void FindLargest();

            const Comparator* comparator_;
            // 子迭代器数组
            IteratorWrapper* children_;
            // 子迭代器数量
            int n_;

            // 指向当前的迭代器
            IteratorWrapper* current_;
            // 移动方向
            Direction direction_;
        };

        void MergingIterator::FindSmallest() {
            IteratorWrapper* smallest = nullptr;
            for(int i = 0; i < n_; i++) {
                IteratorWrapper* child = &children_[i];
                if(child->Valid()) {
                    if(smallest == nullptr) {
                        smallest = child;
                    } else if(comparator_->Compare(child->key(), smallest->key()) <= 0) {
                        smallest = child;
                    }
                }
            }
            current_ = smallest;
        }

        void MergingIterator::FindLargest() {
            IteratorWrapper* largest = nullptr;
            for(int i = 0; i < n_; i++) {
                IteratorWrapper* child = &children_[i];
                if(largest == nullptr) {
                    largest = child;
                } else if(comparator_->Compare(child->key(), largest->key()) > 0) {
                    largest = child;
                }
            }
            current_ = largest;
        }

    } // end namespace

    Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                                 int n) {
        assert(n >= 0);
        if(n == 0) {
            return NewEmptyIterator();
        } else if(n == 1) {
            return children[0];
        } else {
            return new MergingIterator(comparator, children, n);
        }
    }

} // end namespace leveldb

