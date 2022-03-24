#include "util/coding.h"

/**
 * @brief uint32_t类型占用4个字节，uint64_t占用8个字节，但是对于较小的数字来说，使用uint32_t和uint64_t来存储
 *        比较浪费空间，Varint的思想是按需存储，根据其大小使用unsigned char*指针来存储，以节约内存；
 * 实现原理：在Varint中每个字节的最高位用来标识此整数是否结束，未结束置1，因此每个字节中有7位可以用来存储数据
 * 
 */

namespace leveldb {

    // ====================================  Put函数  ==========================================================
    // 例如：PutFixed32表示将value按照Fixed32格式编码，然后放到*dst中去

    void PutFixed32(std::string* dst, uint32_t value) {
        char buffer[sizeof(value)];
        EncodeFixed32(buffer, value);
        dst->append(buffer, sizeof(buffer)); // 将buffer中的数据添加到dst中
    }

    void PutFixed64(std::string* dst, uint64_t value) {
        char buffer[sizeof(value)];
        EncodeFixed64(buffer, value);
        dst->append(buffer, sizeof(buffer));
    }

    void PutVarint32(std::string* dst, uint32_t value) {
        // buffer指向第一个地址
        char buffer[5]; 
        // 返回的ptr是buffer指向的地址不断递增的结果，因为Varint是变长的，其指向存储数据的最后那个地址
        char* ptr = EncodeVarint32(buffer, value); 
        dst->append(buffer, ptr-buffer); // Varint32的实际长度用ptr-buffer来计算
    }

    void PutVarint64(std::string* dst, uint64_t value) {
        char buffer[10];
        char* ptr = EncodeVarint64(buffer, value);
        dst->append(buffer, ptr-buffer);
    }
    // 此函数将Slice数据的长度编码为Varint32格式后作为前缀放在其前面
    void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
        // 把value的长度编码为Varint32并将其放到dst指针指向的地址中，其中value是Slice型数据，其长度也就是Slice型数据的长度
        PutVarint32(dst, value.size());
        // 最后将value的数据也添加到dst中，与之前放进去的value数据长度拼接在一起，
        // 不过value的数据长度(size)是经过Variant32编码过的，而value数据(data)就是单纯的原始数据，
        // 因此，此时dst指针指向的地址中存储的数据形式为：Varint32(value.size) + value.data
        dst->append(value.data(), value.size());
    }

    // ====================================  VarintLength 函数  ================================================ 
    // 计算value编码为Varint格式需要几个字节（即Varint格式的value由多少个字节组成，也即value的Varint格式的长度）
    int VarintLength(uint64_t value) {
        int len = 1; 

        // 比128大表明仍需要更多的字节来存储value的数据
        while (value >= 128) { 
            // 丢掉低7位数据，每个Varint中每个字节有7个有效位来存储数据
            value = value >> 7; 
            len++;
        }
        return len;
    }

    // =========================================  Get函数  =====================================================
    // Get函数描述，从Slice类型数据input中取出一个Varint型数据存放到value指针指向的地址，将input中剩下的数据重新封装为Slice类型

    bool GetVarint32(Slice* input, uint32_t* value) {
        // p指针指向数据的首地址，limit指针指向最后一个数据的尾地址，value用来存取出的数据
        const char* p = input->data();
        const char* limit = p + input->size();
        const char* q = GetVarint32Ptr(p, limit, value);
        if(q == nullptr) {
            return false;
        } else {
            // 从Slice中取出一个Varint32数据后将剩下的数据重新封装成Slice数据，其中q指向剩余数据序列的首地址
            // limit指向尾地址，limit-q就是数据的长度
            *input = Slice(q, limit-q);
            return true;
        }
    }

    bool GerVarint64(Slice* input, uint64_t* value) {
        const char* p = input->data();
        const char* limit = p + input->size();
        const char* q = GetVarint64Ptr(p, limit, value);
        if(q == nullptr) {
            return false;
        } else {
            *input = Slice(q, limit - q);
            return true;
        }
    }

    // 从前面用PutLengthPrefixedSlice函数封装的Slice数据中重新读取出Slice型数据
    const char* GetLengthPrefixedSlice(const char* p, const char* limit, Slice* result) {
        uint32_t len;
        // 前面的PutLengthPrefixedSlice函数是将value的长度编码为Varint32后和value的数据拼接到一块，
        // 这里则是先读取出Varint32编码的value长度数据，将其读取到&len，若读取成功则返回的p指针已经向前移动到value的数据部分
        p = GetVarint32Ptr(p, limit, &len); 
        // 下面是两种异常情况，都直接返回空指针
        if (p == nullptr) return nullptr;
        if (p + len > limit) return nullptr;
        // 根据当前p指针指向的数据部分和其长度len构造Slice型数据
        *result = Slice(p, len);
        return p + len; // 返回数据的尾部地址指针
    }
    // 作用基本同上面的同名函数
    bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
        uint32_t len;
        if (GetVarint32(input, &len) && input->size() >= len) {
            *result = Slice(input->data(), len);
            input->remove_prefix(len);
            return true;
        } else {
            return false;
        }
    }



    // ====================================  Varint编码函数  ====================================================
    /**
     * @brief 对uint32_t数值进行变长编码为Varint32
     * 
     * @param dst 存储转换结果的指针地址
     * @param value 要进行编码的原uint32_t数
     * @return char* 
     */
    char* EncodeVarint32(char* dst, uint32_t value) {
        // 将dst按位转为8位的无符号整型
        uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
        // 定义B=(1000 0000)，用来进行「或」运算，以将每个字节的标识位置1
        static const int B = 128;
        if (value < (1<<7)) { // value < (1<<7)，可以用一个字节中的有效7位存储
            *(ptr++) = value;
        } else if (value < (1 << 14 )) { // value < (1 << 14), 可以用两个字节的有效14位来存储
            *(ptr++) = value | B; // 将value的低7位存到第一个字节，此操作能同时将第一个字节的第8位置1
            *(ptr++) = value >> 7; //将value右移7位（也即扔掉我们已经存好的7位后），再将当前的低7位也即此情况下的最后7位有效位存到第二个字节，因为是最后一个字节所以不需要与B进行或运算来置最高位为1
        } else if (value < (1 << 21 )) {
            *(ptr++) = value | B;
            *(ptr++) = (value >> 7) | B;
            *(ptr++) = value >> 14;
        } else if (value < (1 << 28)) {
            *(ptr++) = value | B;
            *(ptr++) = (value >> 7) | B;
            *(ptr++) = (value >> 14) | B;
            *(ptr++) = value >> 21;
        } else { // 因为4个字节中每个字节需要一位做标识位，所以最多只能存28位数据，若是大于28位的数据就需要5个字节
            *(ptr++) = value | B;
            *(ptr++) = (value >> 7) | B;
            *(ptr++) = (value >> 14) | B;
            *(ptr++) = (value >> 21) | B;
            *(ptr++) = value >> 28;
        }
        return reinterpret_cast<char*>(ptr);
    }

    /**
     * @brief 对uint64_t数值进行变长编码为Varint64
     * 
     * @param dst 存储转换结果的指针地址
     * @param value 要进行编码的原uint64_t数
     * @return char* 
     */
    char* EncodeVarint64(char* dst, uint64_t value) {
        static const int B = 128;
        uint8_t * ptr = reinterpret_cast<uint8_t*>(ptr);
        // 如果按照Varint32那样用if-else分支来转换，则太复杂(Varint最多需要10个字节来存64位有效数据，因此需要10个分支)，因此这里进行简化
        while(value > B) {
            *(ptr++) = value | B;
            value = value >> 7;
        }
        // 将最后小于128的剩余数据位存到最后一个字节中，此时不需要最高位置1，因此不需要与B进行「或」运算
        *(ptr++) = static_cast<uint8_t>(value);
        return reinterpret_cast<char*>(ptr);
    }

    // =============================    Varint类型解码函数    =======================================
    // 参数解释：p指针指向数据的首地址，limit指针指向数据的尾地址，value用来存解码出来的数据

    const char* GetVarint32PtrFallback(const char* p, const char* limit, uint32_t* value) {
        uint32_t result = 0;
        
        for(uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) { // shift需要自增4次（7 * 4 = 28），因为Varint32需要5个字节
            uint32_t byte = *(reinterpret_cast<const uint8_t*>(p));
            p++;
            if(byte & 128) { // byte & 128 不为0，则表示还有后续数据
                result |= ((byte & 127) << shift); // 通过与127进行「与」运算来去掉标识位的1，然后右移进行拼接
            } else {
                result |= (byte << shift); // 没有后续数据了，不需要通过「与」运算来去掉标识位的1，直接拼接
                *value = result; // 将所得结果复制到value指针指向的地址
                return reinterpret_cast<const char*>(p); // 成功取出数据则返回指向下一个连续字节数据首地址的指针
            }
        }

        return nullptr; // 出错则返回空指针
    }

    const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) { 
        uint64_t result = 0;
        for(uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) { // // shift需要自增9次（7 * 9 = 63），因为Varint32需要10个字节
            uint64_t byte = *(reinterpret_cast<const uint8_t*>(p));
            p++;
            if(byte & 128) {
                result |= ((byte & 127) << shift);
            } else {
                result |= (byte << shift);
                *value = result;
                return reinterpret_cast<const char*>(p);
            }
        }
        return nullptr;
    }
} // namespace leveldb