//
// Created by Ap0l1o on 2022/4/5.
//

#include "leveldb/dumpfile.h"

#include <cstdio>

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/write_batch.h"
#include "util/logging.h"

namespace leveldb {

    namespace {

        // 获取文件类型
        bool GuessType(const std::string& fname, FileType* type) {
            size_t pos = fname.rfind('/');
            std::string basename;
            if(pos == std::string::npos) {
                // 没找到分隔符
                basename = fname;
            } else {
                basename = std::string(fname.data() + pos + 1, fname.size() - pos - 1);
            }
            uint64_t ignored;
            // 解析文件名，将获得的文件类型存在type中
            return ParseFileName(basename, &ignored, type);
        }

        class CorruptionReporter : public log::Reader::Reporter {
        public:
            void Corruption(size_t bytes, const Status& status) override {
                std::string r = "corruption: ";
                AppendNumberTo(&r, bytes);
                r += " bytes; ";
                r += status.ToString();
                r.push_back('\n');
                dst_->Append(r);
            }

            WritableFile* dst_;
        };

        // 打印日志文件的内容，对读到的每条记录调用func函数
        Status PrintLogContents(Env* env, const std::string& fname,
                                void (*func)(uint64_t, Slice, WritableFile*),
                                WritableFile* dst) {
            // 打开日志文件，创建一个顺序读文件对象
            SequentialFile* file;
            Status s = env->NewSequentialFile(fname, &file);
            if(!s.ok()) {
                return s;
            }
            // 创建reader来读取文件
            CorruptionReporter reporter;
            reporter.dst_ = dst;
            log::Reader reader(file, &reporter, true, 0);
            // 读数据，对读到的每条记录调用func
            Slice record;
            std::string scratch;
            while(reader.ReadRecord(&record, &scratch)) {
                (*func)(reader.LastRecordOffset(), record, dst);
            }
            delete file;
            return Status::OK();
        }

        // 在WriteBatch中遇到每一个item时调用
        class WriteBatchItemPrinter : public WriteBatch::Handle {
        public:
            void Put(const Slice& key, const Slice& value) override {
                std::string r = " put '";
                AppendEscapedStringTo(&r, key);
                r += "' '";
                AppendEscapedStringTo(&r, value);
                r += "'\n";
                dst_->Append(r);
            }

            void Delete(const Slice& key) override {
                std::string r = " del '";
                AppendEscapedStringTo(&r, key);
                r += "'\n";
                dst_->Append(r);
            }

            WritableFile* dst_;
        };

        // 在kLogFile中每发现一个log record，就调用一次此函数，
        // 每个log record是一个WriteBatch
        static void WriteBatchPrinter(uint64_t pos, Slice record, WritableFile* dst) {
            std::string r = "--- offset ";
            AppendNumberTo(&r, pos);
            r += "; ";
            if(record.size() < 12) {
                r += "log record length ";
                AppendNumberTo(&r, record.size());
                r += " is too small\n";
                dst->Append(r);
                return ;
            }
            // 将record构造为WriteBatch
            WriteBatch batch;
            WriteBatchInternal::SetContents(&batch, record);
            r += "sequence ";
            AppendNumberTo(&r, WriteBatchInternal::Sequence(&batch));
            r.push_back('\n');
            dst->Append(r);
            WriteBatchItemPrinter batch_item_printer;
            batch_item_printer.dst_ = dst;
            Status s = batch.Iterate(&batch_item_printer);
            if(!s.ok()) {
                dst->Append(" error: " + s.ToString() + "\n");
            }
        }

        Status DumpLog(Env* env, const std::string& fname, WritableFile* dst) {
            return PrintLogContents(env, fname, WriteBatchPrinter, dst);
        }

        static void VersionEditPrinter(uint64_t pos, Slice record, WritableFile* dst) {
            std::string r = "--- offset ";
            AppendNumberTo(&r, pos);
            r += "; ";
            VersionEdit edit;
            Status s = edit.DecodeFrom(record);
            if(!s.ok()) {
                r += s.ToString();
                r.push_back('\n');
            } else {
                r += edit.DebugString();
            }
            dst->Append(r);
        }

        Status DumpDescriptor(Env* env, const std::string& fname, WritableFile* dst) {
            return PrintLogContents(env, fname, VersionEditPrinter, dst);
        }

        Status DumpTable(Env* env, const std::string& fname, WritableFile* dst) {
            uint64_t file_size;
            RandomAccessFile* file = nullptr;
            Table* table = nullptr;
            // 获取文件大小
            Status s = env->GetFileSize(fname, &file_size);
            if(s.ok()) {
                // 打开文件，构造一个随机读对象
                s = env->NewRandomAccessFile(fname, &file);
            }

            if(s.ok()) {
                // 打开SSTable文件，读出相关元数据信息构造Table对象
                s = Table::Open(Options(), file, file_size, &table);
            }

            if(!s.ok()) {
                delete table;
                delete file;
                return s;
            }

            ReadOptions ro;
            ro.fill_cache = false;
            // 构造读取SSTable文件的迭代器
            Iterator* iter = table->NewIterator(ro);
            std::string r;
            for(iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                r.clear();
                ParsedInternalKey key;
                if(!ParseInternalKey(iter->key(), &key)) {
                    r = "badkey '";
                    AppendEscapedStringTo(&r, iter->key());
                    r += "' => '";
                    AppendEscapedStringTo(&r, iter->value());
                    r += "'\n";
                    dst->Append(r);
                } else {
                    r = "'";
                    AppendEscapedStringTo(&r, key.user_key);
                    r += "' @";
                    AppendNumberTo(&r, key.sequence);
                    r += " : ";
                    if(key.type == kTypeDeletion) {
                        r += "del";
                    } else if(key.type == kTypeValue) {
                        r += "val";
                    } else {
                        AppendNumberTo(&r, key.type);
                    }
                    r += " => '";
                    AppendEscapedStringTo(&r, iter->value());
                    r += "'\n";
                    dst->Append(r);
                }
            }

            s = iter->status();
            if(!s.ok()) {
                dst->Append("iterator error: " + s.ToString() + "\n");
            }

            delete iter;
            delete table;
            delete file;
            return Status::OK();
        }

    } // end namespace

    Status DumpFile(Env* env, const std::string& fname, WritableFile* dst) {
        FileType ftype;
        if(!GuessType(fname, &ftype)) {
            return Status::InvalidArgument(fname, ": unknown file type");
        }
        switch (ftype) {
            case kLogFile:
                return DumpLog(env, fname, dst);
            case kDescriptorFile:
                return DumpDescriptor(env, fname, dst);
            case kTableFile:
                return DumpTable(env, fname, dst);
            default:
                break;
        }
        return Status::InvalidArgument(fname + ": not a dump-able file type");
    }

} // end namespace leveldb
