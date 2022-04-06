//
// Created by Ap0l1o on 2022/4/5.
//

#ifndef LLEVELDB_WRITE_BATCH_H
#define LLEVELDB_WRITE_BATCH_H

#include <string>

#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

    class Slice;
    // 批量写类，数据先缓存到rep_，然后再批量写
    class LEVELDB_EXPORT WriteBatch {
    public:
        class LEVELDB_EXPORT Handle {
        public:
            virtual ~Handle();
            virtual void Put(const Slice& key, const Slice& value) = 0;
            virtual void Delete(const Slice& key) = 0;
        };

        WriteBatch();
        WriteBatch(const WriteBatch&) = default;
        WriteBatch& operator=(const WriteBatch&) = default;

        ~WriteBatch();

        // 将key->value映射存到数据库
        void Put(const Slice& key, const Slice& value);

        // 如果数据库包含key的映射关系，则删除它
        void Delete(const Slice& key);

        // 清除此WriteBatch中缓存的所有更新
        void Clear();

        // 当前WriteBatch使数据库变化的数据量
        size_t Approximate() const;

        // 将"source"中的操作拷贝到当前WriteBatch
        void Append(const WriteBatch& source);

        // 对一个WriteBatch的内容进行迭代遍历
        Status Iterate(Handle* handle) const;

    private:
        friend class WriteBatchInternal;
        std::string rep_;
    };

} // end namespace leveldb



#endif //LLEVELDB_WRITE_BATCH_H
