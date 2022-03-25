//
// Created by Ap0l1o on 2022/3/25.
//

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

    namespace {
        // 定义函数指针作为回调函数
        typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

        class TwoLevelIterator : public Iterator {
        public:
            TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                             void* arg, const ReadOptions& options);
            ~TwoLevelIterator() override;

            void Seek(const Slice& target) override;
            void SeekToFirst() override;
            void SeekToLast() override;
            void Next() override;
            void Prev() override;

            bool Valid() const override { return data_iter_.Valid(); }
            Slice key() const override {
                assert(Valid());
                return data_iter_.key();
            }
            Slice value() const override {
                assert(Valid());
                return data_iter_.value();
            }

            Status status() const override {
                if(!index_iter_.status().ok()) {
                    return index_iter_.status();
                } else if(data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
                    return data_iter_.status();
                } else {
                    return status_;
                }
            }

        private:
            void SaveError(const Status& s) {
                if(status_.ok() && !s.ok())
                    status_ = s;
            }

            void SkipEmptyDataBlocksForward();
            void SkipEmptyDataBlocksBackward();
            void SetDataIterator(Iterator* data_iter);
            void InitDataBlock();

            BlockFunction block_function_;
            void* arg_;
            const ReadOptions options_;
            Status status_;
            IteratorWrapper index_iter_;
            IteratorWrapper data_iter_;
            std::string data_block_handle;
        };

        TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                           BlockFunction block_function, void* arg,
                                           const ReadOptions& options)
                                           : block_function_(block_function),
                                             arg_(arg),
                                             options_(options),
                                             index_iter_(index_iter),
                                             data_iter_(nullptr) {}

        TwoLevelIterator::~TwoLevelIterator() = default;

        void TwoLevelIterator::Seek(const Slice &target) {
            // 先找到目标key可能在的data block 的索引，也即handle
            index_iter_.Seek(target);
            // 根据handle初始化 data block iterator
            InitDataBlock();
            // 在data block iterator中查找目标key
            if(data_iter_.iter() != nullptr) {
                data_iter_.Seek(target);
            }
            SkipEmptyDataBlocksForward();
        }

        void TwoLevelIterator::SeekToFirst() {
            // 移动到第一个data block的handle
            index_iter_.SeekToFirst();
            // 根据handle初始化data block
            InitDataBlock();
            // 查找第一个data block的第一个KV
            if(data_iter_.iter() != nullptr) {
                data_iter_.SeekToFirst();
            }
            SkipEmptyDataBlocksForward();
        }

        void TwoLevelIterator::SeekToLast() {
            // 移动到最后一个data block的handle
            index_iter_.SeekToLast();
            // 根据handle初始化data block
            InitDataBlock();
            // 查找最后一个data block的最后一个KV
            if(data_iter_.iter() != nullptr) {
                data_iter_.SeekToLast();
            }
            SkipEmptyDataBlocksBackward();
        }

        void TwoLevelIterator::Prev() {
            assert(Valid());
            data_iter_.Prev();
            SkipEmptyDataBlocksBackward();
        }

        void TwoLevelIterator::Next() {
            assert(Valid());
            data_iter_.Next();
            SkipEmptyDataBlocksForward();
        }

        // 向前跳过空data block
        void TwoLevelIterator::SkipEmptyDataBlocksForward() {
            // 当前data block 为空， 则移动到下一个data block
            while(data_iter_.iter() == nullptr && !data_iter_.Valid()) {
                // 没有下一个data block了，则将data block 置空，并返回
                if(!index_iter_.Valid()) {
                    SetDataIterator(nullptr);
                    return ;
                }
                // 先从index block获取到下一个data block的block handle
                index_iter_.Next();
                // 根据index block iterator当前指向 data block handle来初始化data block iterator
                InitDataBlock();
                // data block iterator移动到下一个data block后，将指针指向其第一个KV对
                if(data_iter_.iter() != nullptr) {
                    data_iter_.SeekToFirst();
                }
            }
        }

        // 向后跳过空data block
        void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
            // 当前data block 为空， 则移动到前一个data block
            while(data_iter_.iter() == nullptr || !data_iter_.Valid()) {
                if(!index_iter_.Valid()) {
                    SetDataIterator(nullptr);
                    return ;
                }
                // 先从index block获取到前一个data block的block handle
                index_iter_.Prev();
                // 根据index block iterator当前指向 data block handle来初始化data block iterator
                InitDataBlock();
                // data block iterator移动到前一个data block后，将其指针指向其最后一个KV对
                if(data_iter_.iter() != nullptr) {
                    data_iter_.SeekToLast();
                }
            }
        }

        // 设置data block iterator
        void TwoLevelIterator::SetDataIterator(Iterator *data_iter) {
            if(data_iter_.iter() != nullptr) {
                SaveError(data_iter_.status());
            }
            data_iter_.Set(data_iter);
        }

        // 根据index block iterator指向的data block handle来初始化data block
        void TwoLevelIterator::InitDataBlock() {
            if(!index_iter_.Valid()) {
                // index block iterator无效
                SetDataIterator(nullptr);
            } else {
                // 获取当前index block iterator指向的data block handle
                Slice handle = index_iter_.value();
                if(data_iter_.iter() != nullptr && handle.compare(data_block_handle) == 0) {
                    // handle相同，data block iterator不需要改变
                } else {
                    // 根据handle获取data block的读取迭代器
                    Iterator* iter = (*block_function_)(arg_, options_, handle);
                    data_block_handle.assign(handle.data(), handle.size());
                    // 设置data block的读取迭代器
                    SetDataIterator(iter);
                }
            }
        }

    } // end namespace

    Iterator* NewTwoLevelIterator(Iterator* index_iter,
                                  BlockFunction block_function, void* arg,
                                  const ReadOptions& options) {
        return new TwoLevelIterator(index_iter, block_function, arg, options);
    }


} // end namespace leveldb