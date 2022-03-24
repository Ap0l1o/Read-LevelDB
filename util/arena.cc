#include "util/arena.h"
/**
 * @brief C++提供new/delete来管理内存的申请和释放，但是对于小对象来说，直接使用new/delete代价比较大，要付出
 *        额外的空间和时间，性价比不高。另外，也需要避免多次申请和释放引起的内存碎片，一旦碎片到达一定程度，即使
 *        剩余内存够用，但由于缺乏足够的连续空闲内存空间，也会导致内存不够用的假象。
 *        注：内存池的存在主要就是为了减少内存分配所带来的系统开销，以提高性能。
 * 原理：为避免小对象的频繁内存分配，需要减少对new的使用，最简单的做法就是申请大块的内存，多次分给用户。
 *      LevelDB使用一个「vector<char*>」来保存所有内存分配记录表，默认每次申请4K的内存，并记录剩余指针和剩余
 *      内存字节数。每当有新的申请，如果当前剩余的字节能满足需求，则直接返回给用户，否则，对于超过1K的申请，直接new
 *      申请相应的字节数量并返回，小于1K的申请，则申请一个新的4K块，从中分配一部分给用户。
 * 
 */


namespace leveldb {
    // 定义block大小为4KB ( 4096 = 1024 * 4)
    static const int kBlockSize = 4096;

    Arena::Arena() : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}
    Arena::~Arena() {
        // 释放申请的内存空间
        for(size_t i = 0; i < blocks_.size(); i++) {
            delete[] blocks_[i];
        }  
    } // end ~Arena

    // 当前内存块剩余内存不够分配，使用该分配函数进行内存分配（需要向操作系统申请内存空间）
    char* Arena::AllocateFallback(size_t bytes) {
        // 如果超过「1/4」的block大小则单独进行分配，以避免在剩余字节中浪费太多空间
        // 1. 超过「1/4」的block大小，也即超过1K的内存申请，直接用new申请相应的字节并返回
        if(bytes > kBlockSize / 4) {
            char* result = AllocateNewBlock(bytes);
            return result;
        }
        // 以下分配方式会浪费当前block的剩余空间
        // 2. 小于1K的内存申请，则申请一个新的4K的block，从中分配一部分给用户，
        //    并针对每个block都记录剩余字节数
        alloc_ptr_ = AllocateNewBlock(kBlockSize); // 分配一个4K的block
        alloc_bytes_remaining_ = kBlockSize; // 记录当前内存块的剩余内存大小

        char* result = alloc_ptr_; // 指向分配的地址首部
        alloc_ptr_ += bytes; // 从刚分配的当前内存块中拿出一部分来给用户，分配指针相应的需要前移
        alloc_bytes_remaining_ -= bytes; // 计算当前内存块的剩余内存大小
        return result;
    } // end AllocateFallback

    // 使用该分配函数分配对齐的内存
    char* Arena::AllocateAligned(size_t bytes) {
        // 计算当前机器要对齐的字节数
        // sizeof(void*) 计算空指针的大小，空指针的大小因系统而异。如果系统是16位，则void指针的大小
        // 为2个字节；如果系统是32位，则void指针的大小为4个字节；如果系统是64位，则void指针的大小为8个字节。
        const int align = ( sizeof(void*) > 8 ) ? sizeof(void*) : 8;
        // 字节对齐，也即align的大小必须为2的幂
        // align & (align-1) == 0 则align为2的幂
        static_assert( (align & (align-1)) == 0 ,
                        "Pointer size should be a power of 2");
        
        // 用当前内存块未分配内存的起始地址来对对齐大小求余运算
        // A & (B - 1) = A % B
        size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
        // current_mode为0表示alloc_ptr_已经是内存对齐的，否则，则计算还需要多少字节可以字节对齐
        size_t slop = ( current_mod == 0 ? 0 : align - current_mod );
        // 需要分配的字节数 = 申请的字节数 + 距离字节对齐所需要的字节数
        size_t needed = bytes + slop; 
        char* result;
        // 1. 所需内存小于等于当前内存块剩余大小，直接在当前内存块上分配内存
        if(needed <= alloc_bytes_remaining_) {
            result = alloc_ptr_ + slop;
            alloc_ptr_ += needed;
            alloc_bytes_remaining_ -= needed;
        } else {
            // 2. 否则直接重新申请内存
            result = AllocateNewBlock(bytes);
        }
        // 确保分配的内存的起始地址是字节对齐的
        assert( ( reinterpret_cast<uintptr_t>(result) & ( align - 1 ) ) == 0 );
        return result;

    } // end AllocateAligned


    // 申请一个大小为block_bytes的内存块
    char* Arena::AllocateNewBlock(size_t block_bytes) {
        // 申请一个大小为block_bytes的内存块
        char* result = new char[block_bytes];
        // 将该内存块的地址加入到blocks_中
        blocks_.push_back(result);
        // 记录当前对象内存分配总量，包括 分配的内存空间大小(block_bytes) + 指向内存块的指针大小(char*) 之和。
        memory_usage_.fetch_add(block_bytes + sizeof(char*), std::memory_order_relaxed);
        return result;
    }


}