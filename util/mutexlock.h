//
// Created by Ap0l1o on 2022/3/26.
//

#ifndef LLEVELDB_MUTEXLOCK_H
#define LLEVELDB_MUTEXLOCK_H

#include "port/port.h"
#include "port/thread_annotations.h"
#include "port/port_stdcxx.h"

namespace leveldb {

    class SCOPED_LOCKABLE MutexLock {
    public:
        explicit MutexLock(port::Mutex* mu) EXCLUSIVE_LOCK_FUNCTION(mu) : mu_(mu) {
            this->mu_->Lock();
        }
        ~MutexLock() UNLOCK_FUNCTION() { this->mu_->Unlock(); }

        MutexLock(const MutexLock&) = delete;
        MutexLock& operator=(const MutexLock&) = delete;
    private:
        port::Mutex* const mu_;
    };

} // end namespace leveldb

#endif //LLEVELDB_MUTEXLOCK_H
