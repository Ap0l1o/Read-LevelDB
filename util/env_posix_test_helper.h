//
// Created by Ap0l1o on 2022/3/30.
//

#ifndef LLEVELDB_ENV_POSIX_TEST_HELPER_H
#define LLEVELDB_ENV_POSIX_TEST_HELPER_H

namespace leveldb {

    class EnvPosixTest;

    // A helper for the POSIX Env to facilitate testing.
    class EnvPosixTestHelper {
    private:
        friend class EnvPosixTest;

        // Set the maximum number of read-only files that will be opened.
        // Must be called before creating an Env.
        static void SetReadOnlyFDLimit(int limit);

        // Set the maximum number of read-only files that will be mapped via mmap.
        // Must be called before creating an Env.
        static void SetReadOnlyMMapLimit(int limit);
    };

}  // namespace leveldb

#endif //LLEVELDB_ENV_POSIX_TEST_HELPER_H
