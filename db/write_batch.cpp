//
// Created by Ap0l1o on 2022/4/5.
//

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "util/coding.h"


// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

namespace leveldb {

    // WriteBatch header包括8-byte的sequence number和4-byte的count
    static const size_t kHeader = 12;

    WriteBatch::WriteBatch() { Clear(); }

    WriteBatch::~WriteBatch() = default;

    WriteBatch::Handle::~Handle() = default;

    void WriteBatch::Clear() {
        rep_.clear();
        rep_.resize(kHeader);
    }

    size_t WriteBatch::Approximate() const {
        return rep_.size();
    }

    // 对当前WriteBatch进行迭代遍历，并将数据通过handle插入到MemTable
    Status WriteBatch::Iterate(Handle *handle) const {
        // WriteBatch的数据源是rep_
        Slice input(rep_);
        if(input.size() < kHeader) {
            return Status::Corruption("malformed WriteBatch (too small)");
        }

        // 去掉前缀（header），保留数据部分
        input.remove_prefix(kHeader);
        Slice key, value;
        // 记录record的数量
        int found = 0;
        while(!input.empty()) {
            found++;
            // 每条record以类型开头，获取record类型
            char tag = input[0];
            // 去掉类型
            input.remove_prefix(1);
            switch (tag) {
                case kTypeValue:
                    if(GetLengthPrefixedSlice(&input, &key) &&
                       GetLengthPrefixedSlice(&input, &value) ) {
                        handle->Put(key, value);
                    } else {
                        return Status::Corruption("bad WriteBatch Put");
                    }
                    break;
                case kTypeDeletion:
                    if(GetLengthPrefixedSlice(&input, &key)) {
                        handle->Delete(key);
                    } else {
                        return Status::Corruption("bad WriteBatch Delete");
                    }
                    break;
                default:
                    return Status::Corruption("unknown WriteBatch tag");
            }
            // 实际读取的数量与查询到的数量不一致
            if(found != WriteBatchInternal::Count(this)) {
                return Status::Corruption("WriteBatch has wrong count");
            } else {
                return Status::OK();
            }
        }

    }

    int WriteBatchInternal::Count(const WriteBatch *b) {
        // header: 8-byte sequence number + 4-byte count
        return DecodeFixed32(b->rep_.data() + 8);
    }

    void WriteBatchInternal::SetCount(WriteBatch *b, int n) {
        EncodeFixed32(&b->rep_[8], n);
    }

    SequenceNumber WriteBatchInternal::Sequence(const WriteBatch *b) {
        return SequenceNumber(DecodeFixed64(b->rep_.data()));
    }

    void WriteBatchInternal::SetSequence(WriteBatch *b, SequenceNumber seq) {
        EncodeFixed64(&b->rep_[0], seq);
    }

    // 将key->value数据映射写到rep_
    void WriteBatch::Put(const Slice &key, const Slice &value) {
        // 将key->value数量加1
        WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
        rep_.push_back(static_cast<char>(kTypeValue));
        PutLengthPrefixedSlice(&rep_, key);
        PutLengthPrefixedSlice(&rep_, value);
    }

    // 将删除信息写到rep_
    void WriteBatch::Delete(const Slice &key) {
        WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
        rep_.push_back(static_cast<char>(kTypeDeletion));
        PutLengthPrefixedSlice(&rep_, key);
    }

    void WriteBatch::Append(const WriteBatch &source) {
        WriteBatchInternal::Append(this, &source);
    }

    namespace {
        // 用于向MemTable中插入数据
        class MemTableInserter : public WriteBatch::Handle {
        public:
            SequenceNumber sequence_;
            MemTable* mem_;

            void Put(const Slice& key, const Slice& value) override {
                mem_->Add(sequence_, kTypeValue, key, value);
                sequence_++;
            }

            void Delete(const Slice& key) override {
                mem_->Add(sequence_, kTypeDeletion, key, Slice());
                sequence_++;
            }
        };

    } // end namespace

    // 将WriteBatch b的数据插入到MemTable memtable
    Status WriteBatchInternal::InsertInto(const WriteBatch *b, MemTable *memtable) {
        MemTableInserter inserter;
        // 获取序号
        inserter.sequence_ = WriteBatchInternal::Sequence(b);
        inserter.mem_ = memtable;
        // 遍历WriteBatch b，并通过MemTableInserter inserter 将数据插入到MemTable memtable
        return b->Iterate(&inserter);
    }

    void WriteBatchInternal::SetContents(WriteBatch *b, const Slice &contents) {
        assert(contents.size() >= kHeader);
        b->rep_.assign(contents.data(), contents.size());
    }

    void WriteBatchInternal::Append(WriteBatch *dst, const WriteBatch *src) {
        SetCount(dst, Count(dst) + Count(src));
        assert(src->rep_.size() >= kHeader);
        dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
    }

} // end namespace leveldb