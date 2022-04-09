//
// Created by Ap0l1o on 2022/4/6.
//

#ifndef LLEVELDB_DB_ITER_H
#define LLEVELDB_DB_ITER_H

#include <cstdint>

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace leveldb {

    class DBImpl;

    Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator,
                            Iterator* internal_iter, SequenceNumber sequence,
                            uint32_t seed);

} // end namespace leveldb



#endif //LLEVELDB_DB_ITER_H
