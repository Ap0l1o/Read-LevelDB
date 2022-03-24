#include "leveldb/iterator.h"

namespace leveldb {
    Iterator::Iterator() {
        cleanup_head_.function = nullptr;
        cleanup_head_.next = nullptr;
    }

    Iterator::~Iterator() {
        if(!cleanup_head_.IsEmpty()) {
            // 调用cleanup函数
            cleanup_head_.Run();
            for(CleanupNode* node = cleanup_head_.next; node!=nullptr;) {
                node->Run();
                CleanupNode* next_node = node->next;
                delete node;
                node = next_node;
            }
        }
    }

    void Iterator::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
        assert(func != nullptr);
        CleanupNode* node;
        // 若头节点为空，则赋值给头节点，否则创建后继节点
        if(cleanup_head_.IsEmpty()) {
            node = &cleanup_head_;
        } else {
            node = new CleanupNode();
            node->next = cleanup_head_.next;
            cleanup_head_.next =node;
        }
        node->function = func;
        node->arg1 = arg1;
        node->arg2 = arg2;
    }

    namespace {
        // EmptyIterator 类，实现所有接口，但全部都是空实现
        class EmptyIterator : public Iterator {
            public:
            EmptyIterator(const Status& s):status_(s) {}
            ~EmptyIterator() override = default;

            bool Valid() const override { return false; }
            void Seek(const Slice& target) override {}
            void SeekToFirst() override {}
            void SeekToLast() override {}
            void Next() override { assert(false); }
            void Prev() override { assert(false); }
            Slice key() const override {
                assert(false);
                return Slice();
            }
            Slice value() const override {
                assert(false);
                return Slice();
            }

            Status status() const override { return status_; }

            private:
            Status status_;
        };
    } // anonymous namespace 匿名命名空间
    // 返回一个空的迭代器，迭代器status为ok
    Iterator* NewEmptyIterator() { return new EmptyIterator(Status::OK()); }
    // 返回一个空的迭代器，迭代器status为错误信息
    Iterator* NewErrorIterator(const Status& status) {
        return new EmptyIterator(status);
    }
}