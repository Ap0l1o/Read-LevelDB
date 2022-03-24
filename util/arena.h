#ifndef ARENA_H_
#define ARENA_H

#include <atomic>
#include <cassert>
#include <cstddef>
#include <vector>

namespace leveldb {
    class Arena {
        public:
        Arena();
        // 类中的默认函数包括：类中默认的成员函数 和 类中自定义的操作符函数
        // 类中的默认成员函数：默认构造函数、默认析构函数、默认构造函数、拷贝构造函数、拷贝赋值函数、移动构造函数、移动拷贝函数
        // 类中被default关键字修饰的默认函数：控制默认函数的生成，显示的指示编译器生成该函数的默认版本。
        // （例如提供带参构造函数后，编译器不会再自动生成默认版本）
        // 删除函数（类中被delete关键字修饰的默认函数）：用于限制默认函数的生成，delete关键字显示指示编译器不生成函数的默认版本
        Arena(const Arena&) = delete;
        Arena& operator=(const Arena&) = delete;

        ~Arena();

        // 返回一个指针，指向分配的指定字节数的内存块地址
        char* Allocate(size_t bytes);
        // 使用 malloc 提供的正常对齐来保证内存分配
        char* AllocateAligned(size_t bytes);

        // 估计Arena分配的数据的总内存使用量
        // 该函数返回当前分配给Arena对象的所有内存空间大小和所有指向内存块的指针大小之和。
        size_t MemoryUsage() const {
            return memory_usage_.load(std::memory_order_relaxed);
        }

        private:
        // 分配函数
        char* AllocateFallback(size_t bytes); // 直接分配内存
        char* AllocateNewBlock(size_t block_bytes); // 分配对齐的内存空间
        // 分配状态
        char* alloc_ptr_; // 指向当前内存块未分配内存的起始地址的指针
        size_t alloc_bytes_remaining_; // 记录当前内存块未分配内存的大小，单位为字节
        // 将申请到的内存block放入一个向量中
        std::vector<char*> blocks_; // 每个内存块的地址都存储在blocks_中

        // arena使用的总内存
        // 原子类型是封装了一个值的类型，它的访问保证不会导致数据的竞争，并且可以用于在不同的线程之间
        // 同步内存访问
        // TODO(costan): This member is accessed via atomics, but the others are
        //               accessed without any locking. Is this OK?
        std::atomic<size_t> memory_usage_; // 原子变量，记录当前对象的内存总量
    };


    // 内存分配
    inline char* Arena::Allocate(size_t bytes) {
        assert(bytes > 0); // 小于等于0则没必要分配
        // 1. 如果「bytes」小于等于当前内存块剩余内存，则直接在当前内存块上分配内存
        if ( bytes <= alloc_bytes_remaining_ ) {
            char* result = alloc_ptr_;
            alloc_ptr_ += bytes; // 从当前内存块中分配内存
            alloc_bytes_remaining_ -= bytes; // 计算当前内存块的剩余内存大小
            return result;
        }
        // 2. 如果「bytes」大于当前内存块剩余内存，调用「AllocateFallback」函数按照另外两种分配策略分配内存
        return AllocateFallback(bytes);
    }
} // namespace leveldb


#endif // AREA_H_