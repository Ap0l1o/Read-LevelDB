#ifndef _RANDOM_H_
#define _RANDOM_H_

#include <cstdint>
namespace leveldb {
    class Random {
        private:
        uint32_t seed_;

        public:
        // 构造函数（禁止隐式转换）
        explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
            // 避免糟糕的随机种子
            if(seed_ == 0 || seed_ == 2147483647L) {
                seed_ = 1;
            }
        }
        
        // 生成随机数
        uint32_t Next() {
            static const uint32_t M = 2147483647L;
            static const uint64_t A = 16807;
            // 计算方式
            // seed_ = ( seed_ * A) % M
            uint64_t product  = seed_ * A;
            
            // 利用 ((x<<31) % M) = x，来计算 product % M
            seed_ = static_cast<uint32_t>((product>>31) + (product & M));

            if(seed_ > M) {
                seed_ -= M;
            } 
            return seed_;
        }
        // 生成0~n-1之前的随机数，n>0
        uint32_t Uniform(int n) {
            return Next() % n;
        }
        // 随机返回true，概率很小，
        bool OneIn(int n) {
            return (Next() % n) == 0;
        }

        uint32_t Skewed(int max_log) {
            return (1 << Uniform(max_log + 1));
        }
    };

} // namespace leveldb

#endif