//
// Created by Ap0l1o on 2022/4/5.
//

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

    // 根据数据输入迭代器iter，在数据库dbname中创建一个SSTable文件，将该SSTable文件的元数据信息
    // 保存在meta中。
    Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                      TableCache* table_cache, Iterator* iter, FileMetaData* meta) {

        Status s;
        meta->file_size = 0;
        iter->SeekToFirst();

        // 根据数据库名和文件编号，获取文件名
        std::string fname = TableFileName(dbname, meta->number);
        if(iter->Valid()) {
            // 根据文件名打开文件，创建写文件对象WritableFile
            WritableFile* file;
            s = env->NewWritableFile(fname, &file);
            if(!s.ok()) {
                return s;
            }
            // 创建一个TableBuilder对象用于创建sstable文件
            TableBuilder* builder = new TableBuilder(options, file);
            // 保存sstable文件的最小key
            meta->smallest.DecodeFrom(iter->key());
            Slice key;
            // 往TableBuilder中添加数据来构造sstable文件
            for(; iter->Valid(); iter->Next()) {
                key = iter->key();
                builder->Add(key, iter->value());
            }
            // 保存sstable文件的最大key
            if(!key.empty()) {
                meta->largest.DecodeFrom(key);
            }

            // 完成sstable文件构建，并检查错误
            s = builder->Finish();
            if(s.ok()) {
                // 保存SSTable的大小
                meta->file_size = builder->FileSize();
                assert(meta->file_size > 0);
            }
            delete builder;

            // 完成文件写入，检查文件错误
            if(s.ok()) {
                file->Sync();
            }
            if(s.ok()) {
                file->Close();
            }
            delete file;
            file = nullptr;

            // 验证创建的sstable文件是否可用
            if(s.ok()) {
                Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                                        meta->file_size);
                s = it->status();
                delete it;
            }

        }

        // 检查数据输入迭代器iter
        if(!iter->status().ok()) {
            s = iter->status();
        }

        if(s.ok() && meta->file_size > 0 ) {
            // 以上过程都没问题，保留该文件
        } else {
            // 出错了，删除刚创建的文件
            env->RemoveFile(fname);
        }

        return s;
    }

} // end namespace leveldb