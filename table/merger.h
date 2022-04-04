//
// Created by Ap0l1o on 2022/4/4.
//

#ifndef LLEVELDB_MERGER_H
#define LLEVELDB_MERGER_H

namespace leveldb {
    class Comparator;
    class Iterator;

    Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                                 int n);

} // end namespace leveldb

#endif //LLEVELDB_MERGER_H
