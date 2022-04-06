//
// Created by Ap0l1o on 2022/4/6.
//

#ifndef LLEVELDB_SNAPSHOT_H
#define LLEVELDB_SNAPSHOT_H

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace leveldb {

    class SnapshotList;

    class SnapshotImpl : public Snapshot {
    public:
        SnapshotImpl(SequenceNumber sequence_number)
            : sequence_number_(sequence_number) {}

        SequenceNumber sequence_number() const {
            return sequence_number_;
        }
    private:
        friend class SnapshotList;
        // SnapShotImpl保存在DB中的一个双向链表中
        SnapshotImpl* prev_;
        SnapshotImpl* next_;

        const SequenceNumber sequence_number_;
#if !defined(NDEBUG)
        SnapshotList* list_ = nullptr;
#endif // !defined(NDEBUG)

    };

    class SnapshotList {
    public:
        SnapshotList() : head_(0) {
            head_.prev_ = &head_;
            head_.next_ = &head_;
        }

        bool empty() const {
            return head_.next_ == &head_;
        }

        SnapshotImpl* oldest() const {
            assert(!empty());
            return head_.next_;
        }

        SnapshotImpl* newest() const {
            assert(!empty());
            return head_.prev_;
        }

        // 创建一个SnapshotImpl，并将其添加到链表的尾部
        SnapshotImpl* New(SequenceNumber sequence_number) {
            assert(empty() || newest()->sequence_number_ <= sequence_number);
            SnapshotImpl* snapshot = new SnapshotImpl(sequence_number);
#if !defined(NDEBUG)
            snapshot->list_ = this;
#endif
            // 插入到双向链表
            snapshot->next_ = &head_;
            snapshot->prev_ = head_.prev_;
            snapshot->prev_->next_ = snapshot;
            snapshot->next_->prev_ = snapshot;
            return snapshot;
        }

        // 从链表中删除一个SnapshotImpl
        void Delete(const SnapshotImpl* snapshot) {
#if !defined(NDEBUG)
            assert(snapshot->list_ == this);
#endif
            snapshot->prev_->next_ = snapshot->next_;
            snapshot->next_->prev_ = snapshot->prev_;
            delete snapshot;
        }

    private:
        SnapshotImpl head_;
    };


} //end namespace leveldb

#endif //LLEVELDB_SNAPSHOT_H
