CMAKE_MINIMUM_REQUIRED(VERSION 3.9)

PROJECT(LLeveldb VERSION 1.23.0 LANGUAGES C CXX)

# C standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_C_STANDARD)
    # This project can use C11, but will gracefully decay down to C89.
    set(CMAKE_C_STANDARD 11)
    set(CMAKE_C_STANDARD_REQUIRED OFF)
    set(CMAKE_C_EXTENSIONS OFF)
endif(NOT CMAKE_C_STANDARD)

# C++ standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_CXX_STANDARD)
    # This project requires C++11.
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

#SET(LEVELDB_PUBLIC_INCLUDE_DIR "include/leveldb")
#SET(LEVELDB_PORT_CONFIG_DIR "include/port")



INCLUDE_DIRECTORIES(
        "."
        "include"
)

ADD_LIBRARY(leveldb "" table/filter_block.cpp include/leveldb/table_builder.h table/table_builder.cpp include/leveldb/env.h util/env.cpp include/leveldb/table.h table/table.cpp include/leveldb/cache.h table/two_level_iterator.h table/two_level_iterator.cpp table/iterator_wrapper.h util/cache.cpp port/thread_annotations.h util/mutexlock.h port/port_stdcxx.h)
TARGET_SOURCES(leveldb
        PRIVATE
        "db/dbformat.cc"
        "db/dbformat.h"
        "db/log_format.h"
        "db/log_reader.cc"
        "db/log_reader.h"
        "db/log_writer.cc"
        "db/log_writer.h"
        "db/memtable.cc"
        "db/memtable.h"
        "db/skiplist.h"
        "port/port.h"
        "table/block_builder.cc"
        "table/block_builder.h"
        "table/filter_block.h"
        "table/block.cc"
        "table/block.h"
        "table/format.cc"
        "table/format.h"
        "table/iterator.cc"
        "util/arena.cc"
        "util/arena.h"
        "util/bloom.cc"
        "util/coding.cc"
        "util/coding.h"
        "util/comparator.cc"
        "util/crc32c.cc"
        "util/crc32c.h"
        "util/filter_policy.cc"
        "util/hash.cc"
        "util/hash.h"
        "util/status.cc"
        "include/leveldb/comparator.h"
        "include/leveldb/filter_policy.h"
        "include/leveldb/iterator.h"
        "include/leveldb/options.h"
        "include/leveldb/slice.h"
        "include/leveldb/status.h"
)