#ifndef CODING_H_
#define CODING_H_

#include<cassert>
#include<cstddef>
#include<cstring>
#include<string>


#include "leveldb/slice.h"


/**
 * @brief 1. Varint32和Varint64都是变长编码，可以按照数据的大小进行编码，分别最高可以编码32位(4B)和64位(8B)的数据；
 *        2. Fixed32和Fixed64都是固定长度编码，分别用32位和64位进行编码；
 * 
 */

namespace leveldb {

    //============================  Put函数: 将数据按照指定的编码格式放到字符串中  =======================================
    void PutFixed32(std::string* dst, uint32_t value);
    void PutFixed64(std::string* dst, uint64_t value);
    void PutVarint32(std::string* dst, uint32_t value);
    void PutVarint64(std::string* dst, uint64_t value);
    void PutLengthPrefixedSlice(std::string* dst, const Slice& value);

    //===================================  VarintLength函数：计算编码为Varint所需字节数  =============================
    int VarintLength(uint64_t value);

    //========================  Get函数：从Slice中取出指定格式（eg. Varint32 & Varint64）的数据  =======================
    bool GetVarint32(Slice* input, uint32_t* value);
    bool GetVarint64(Slice* input, uint64_t* value);
    bool GetLengthPrefixedSlice(Slice* input, Slice* result);


    //===========================================  Varint编码函数  =================================================
    char* EncodeVarint32(char *dst, uint32_t value);
    char* EncodeVarint64(char* dst, uint64_t value);


    //==========================================  Varint解码函数  ==================================================
    const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value); // 仅用一个字节编码的数据用该函数解码
    const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value); // 因为Varint64最小都比Varint32大，所以不想Varint32分两个函数来解码
    const char* GetVarint32PtrFallback(const char* p, const char* limit, uint32_t* value); // 多个字节编码的数据用该函数解码

    // 因为该函数比较简单，所以被设置为内联函数
    inline const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value) {
        if(p < limit) {
            uint32_t result = *(reinterpret_cast<const uint8_t*>(p));
            // 这说明，该字节的标识位为0，没有后续字节存储了数据，也即该数据仅用了一个字节来存储(数值小于128)
            if( (result & 128) == 0 ) { 
                *value = result; // 将数据存到value指针指向的地址
                return p+1;
            }
        }
        // 在数据由多个字节进行存储时，用GetVarint32PtrFallback进行遍历处理
        return GetVarint32PtrFallback(p, limit, value);
    }

    //=====================================  Fixed编码函数  ===============================================
    inline void EncodeFixed32(char* dst, uint32_t value) {

        // dst指针和buffer指针指向的地址相同，但是解释不同，也即buffer指针将其解释为uint8_t类型数据的地址
        // 而dst指针将其解释为char类型数据的地址，不过地址还是同一个地址（也即指针的值是完全一样的）
        uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst); 

        buffer[0] = static_cast<uint8_t>(value); // 前8位数据放到buffer[0]
        buffer[1] = static_cast<uint8_t>(value >> 8); // 第二个8位数据放到buffer[1]
        buffer[2] = static_cast<uint8_t>(value >> 16);
        buffer[3] = static_cast<uint8_t>(value >> 24);
    }

    inline void EncodeFixed64(char* dst, uint64_t value) {

        uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

        buffer[0] = static_cast<uint8_t>(value);
        buffer[1] = static_cast<uint8_t>(value >> 8);
        buffer[2] = static_cast<uint8_t>(value >> 16);
        buffer[3] = static_cast<uint8_t>(value >> 24);
        buffer[4] = static_cast<uint8_t>(value >> 32);
        buffer[5] = static_cast<uint8_t>(value >> 40);
        buffer[6] = static_cast<uint8_t>(value >> 48);
        buffer[7] = static_cast<uint8_t>(value >> 56);
    }

    //================================  Fixed解码函数(这里是小端的版本，即数据的低位字节存在低地址端)  ============================
    inline uint32_t DecodeFixed32(const char* ptr) {

        const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);
        
        // 数据位依次左移，通过「或」运算拼接在一起
        return (static_cast<uint32_t>(buffer[0])) | 
               (static_cast<uint32_t>(buffer[1]) << 8) |
               (static_cast<uint32_t>(buffer[2]) << 16) |
               (static_cast<uint32_t>(buffer[3]) << 24);
    }

    inline uint64_t DecodeFixed64(const char* ptr) {

        const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);
        
        // 数据位依次左移，通过「或」运算拼接在一起
        return (static_cast<uint64_t>(buffer[0])) | 
               (static_cast<uint64_t>(buffer[1]) << 8) |
               (static_cast<uint64_t>(buffer[2]) << 16) |
               (static_cast<uint64_t>(buffer[3]) << 24) |
               (static_cast<uint64_t>(buffer[4]) << 32) |
               (static_cast<uint64_t>(buffer[5]) << 40) |
               (static_cast<uint64_t>(buffer[6]) << 48) |
               (static_cast<uint64_t>(buffer[7]) << 56);
    }


} // namespace leveldb

#endif // CODING_H_