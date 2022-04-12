//
// Created by Ap0l1o on 2022/4/9.
//

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"

namespace leveldb {

    Options::Options() : comparator(BytewiseComparator()), env(Env::Default()) {}

} // end namespace leveldb
