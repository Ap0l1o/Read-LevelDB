// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.
// 
// 迭代器接口，用于遍历 key/value 对

#ifndef ITERATOR_H
#define ITERATOR_H
#include "leveldb/export.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {
    class LEVELDB_EXPORT Iterator {
        public:
        Iterator();
        Iterator(const Iterator&) = delete;
        Iterator& operator=(const Iterator&) = delete;

        // 虚函数：允许用基类的指针来调用子类的这个函数
        virtual ~Iterator();

        // 纯虚函数：表示此函数未被实现，继承该类的函数必须实现此函数，否则无法创建此类的实例
        // 迭代器要么指向一个键值对，要么无效，如果迭代器确实指向一个键值对的话则返回 true
        virtual bool Valid() const = 0;
        // 指向数据源的第一个key，若数据源非空的话，Valid()为true
        virtual void SeekToFirst() = 0;
        // 指向数据源的最后一个key，若数据源非空的话，Valid()为true
        virtual void SeekToLast() = 0;
        // 定位数据源中大于等于target的第一个key，若数据源中含有满足条件的key的话，Valid()为true
        virtual void Seek(const Slice& target) = 0;
        // 向前移动，若iterator不指向第一个key的话，Valid()为true
        virtual void Next() = 0;
        virtual void Prev() = 0;
        // Moves to the previous entry in the source.  After this call, Valid() is
        // true iff the iterator was not positioned at the first entry in source.
        // REQUIRES: Valid()
        virtual Slice key() const = 0;
        // Return the value for the current entry.  The underlying storage for
        // the returned slice is valid only until the next modification of
        // the iterator.
        // REQUIRES: Valid()
        virtual Slice value() const = 0;
        // If an error has occurred, return it.  Else return an ok status.
        virtual Status status() const = 0;
        // Clients are allowed to register function/arg1/arg2 triples that
        // will be invoked when this iterator is destroyed.
        //
        // Note that unlike all of the preceding methods, this method is
        // not abstract and therefore clients should not override it.
        // 定义一个函数指针
        using CleanupFunction = void (*)(void* arg1, void* arg2); 
        void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);

        private:
        // Cleanup functions are stored in a single-linked list.
        // The list's head node is inlined in the iterator.
        // Cleanup函数存在一个单链表中，单链表的头节点内联在迭代器中
        struct CleanupNode {
            // True if the node is not used. Only head nodes might be unused.
            // 如果该节点未使用的话返回true，需要注意的是只有头节点可能未被使用
            bool IsEmpty() const {
                return function == nullptr;
            }
            // 调用cleanup函数
            void Run() {
                assert(function != nullptr);
                (*function)(arg1, arg2);
            }
            // 若该函数指针非空的话则该头节点已被使用
            CleanupFunction function;
            void* arg1;
            void* arg2;
            CleanupNode* next;
        };

        CleanupNode cleanup_head_;
    };
    // 返回一个空的迭代器
    LEVELDB_EXPORT Iterator* NewEmptyIterator();
    // 返回一个空的迭代器，并带有特定的状态
    LEVELDB_EXPORT Iterator* NewErrorIterator(const Status& status);
} // namespace leveldb

#endif // ITERATOR_H_