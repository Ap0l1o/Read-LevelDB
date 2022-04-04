//
// Created by Ap0l1o on 2022/3/31.
//

#ifndef LLEVELDB_VERSION_EDIT_H
#define LLEVELDB_VERSION_EDIT_H

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "leveldb/status.h"

namespace leveldb {

    class VersionSet;

    // 用于描述一个SSTable的信息，记录了SSTable元数据
   struct FileMetaData {
       FileMetaData() : refs(0), allowed_seeks(1<<30), file_size(0) {}

       int refs;
       // 是否允许遍历，仅当Compaction时不允许遍历
       int allowed_seeks;
       // 用于唯一标识一个SSTable
       uint64_t number;
       // 文件大小，单位为字节
       uint64_t file_size;
       // SSTable中的最小key
       InternalKey smallest;
       // SSTable中的最大key
       InternalKey largest;
   };

   // VersionEdit记录了Version之间的变化，相当于Version的增量，
   // 每次文件有变动时，leveldb就把变动记录到一个VersionEdit变量中，
   // 然后通过VersionEdit把变动应用到current version上，并把current version的快照，
   // 也就是db元信息保存到`MANIFEST`文件中。
   class VersionEdit {
   public:
       VersionEdit() {
           Clear();
       }
       ~VersionEdit() = default;

       void Clear();

       void SetComparatorName(const Slice& name) {
           has_comparator_ = true;
           comparator_ = name.ToString();
       }

       void SetLogNumber(uint64_t num) {
           has_log_number_ = true;
           has_log_number_ = num;
       }

       void SetPrevLogNumber(uint64_t num) {
           has_prev_log_number_ = true;
           prev_log_number_ = num;
       }

       void SetNextFile(uint64_t num) {
           has_next_file_number_ = true;
           next_file_number_ = num;
       }

       void SetLastSequence(SequenceNumber seq) {
           has_last_sequence_ = true;
           last_sequence_ = seq;
       }

       void SetCompactPointer(int level, const InternalKey& key) {
           compact_pointers_.push_back(std::make_pair(level, key));
       }

       /**
        * 添加一个SSTable文件
        * @param level SSTable所在的level
        * @param file 文件编号
        * @param file_size 文件大小
        * @param smallest 文件最大key
        * @param largest 文件最小key
        */
       void AddFile(int level, uint64_t file, uint64_t file_size,
                    const InternalKey& smallest, const InternalKey& largest) {
           FileMetaData f;
           f.number = file;
           f.file_size = file_size;
           f.smallest = smallest;
           f.largest = largest;
           new_files_.push_back(std::make_pair(level, f));
       }

       /**
        * 删除一个SSTable文件
        * @param level 文件所在level
        * @param file 文件编号
        */
       void RemoveFile(int level, uint64_t file) {
           deleted_files_.insert(std::make_pair(level, file));
       }

       void EncodeTo(std::string* dst) const;
       Status DecodeFrom(const Slice& src);

       std::string DebugString() const;

   private:
       friend class VersionSet;
       typedef std::set<std::pair<int, uint64_t>> DeleteFileSet;

       // key comparator 的名字
       std::string comparator_;
       // 日志编号
       uint64_t log_number_;
       // 前一个日志编号
       uint64_t prev_log_number_;
       // 下一个文件编号
       uint64_t next_file_number_;
       // 上一个序号seq
       SequenceNumber last_sequence_;
       bool has_comparator_;
       bool has_log_number_;
       bool has_prev_log_number_;
       bool has_next_file_number_;
       bool has_last_sequence_;

       // 存放这个version的压缩指针，pair.first对应哪一个level， pair.second 对应哪一个key开始compaction
       std::vector<std::pair<int, InternalKey>> compact_pointers_;
       // 删除文件集合
       DeleteFileSet deleted_files_;
       // 新增文件集合
       std::vector<std::pair<int, FileMetaData>> new_files_;

   };

} // end namespace leveldb

#endif //LLEVELDB_VERSION_EDIT_H
