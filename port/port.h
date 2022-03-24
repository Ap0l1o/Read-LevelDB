#ifndef PORT_H_
#define PORT_H_

#include <string.h>

// 包含相应平台的特定文件
#if defined(LEVELDB_PLATFORM_POSIX) || defined(LEVELDB_PLATFORM_WINDOWS)
#include "port/port_stdcxx.h"
#elif defined(LEVELDB_PLATFORM_CHROMIUM)
#include "port/port_chromium.h"
#endif

#endif // PORT_H_