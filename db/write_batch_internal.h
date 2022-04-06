//
// Created by Ap0l1o on 2022/4/5.
//

#ifndef LLEVELDB_WRITE_BATCH_INTERNAL_H
#define LLEVELDB_WRITE_BATCH_INTERNAL_H

#include "db/dbformat.h"
#include "leveldb/write_batch.h"

namespace leveldb {

    class MemTable;

    // 为了避免暴露WriteBatch中的接口，用WriteBatchInternal提供的静态方法来操作WriteBatch。
    class WriteBatchInternal {
    public:
        // 返回batch中的entry数量
        static int Count(const WriteBatch* batch);

        // 将batch中的entry数量设置为n
        static void SetCount(WriteBatch* batch, int n);

        // 获取batch的起始序号
        static SequenceNumber Sequence(const WriteBatch* batch);

        static void SetSequence(WriteBatch* batch, SequenceNumber seq);

        static Slice Contents(const WriteBatch* batch, SequenceNumber seq) {
            return Slice(batch->rep_);
        }

        static size_t ByteSize(const WriteBatch* batch) {
            return batch->rep_.size();
        }

        static void SetContents(WriteBatch* batch, const Slice& contents);

        static Status InsertInto(const WriteBatch* batch, MemTable* memTable);

        static void Append(WriteBatch* batch, const WriteBatch* src);
    };

} // end namespace leveldb;


#endif //LLEVELDB_WRITE_BATCH_INTERNAL_H
