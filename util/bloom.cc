#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {
    namespace {
        // 计算hash值
        static uint32_t BloomHash(const Slice& key) {
            return Hash(key.data(), key.size(), 0xbc9f1d34);
        }
        // 
        class BloomFilterPolicy : public FilterPolicy {
            public:
            // 初始化
            explicit BloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key) {
                // 四舍五入以减少哈希函数探测成本
                k_ = static_cast<size_t>(bits_per_key * 0.69); // 0.69 =~ ln(2)
                if(k_ < 1) k_ = 1;
                if(k_ > 30) k_ = 30;
            }
            const char* Name() const override { return "leveldb.BuiltinBloomFilter2"; }

            // 创建一个布隆过滤器，将所得的BF存到dst
            void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
                // 计算布隆过滤器的大小，单位为bit
                size_t bits  = n * bits_per_key_;
                // 当n较小时，其误报率较高，因此将其填充到最小为64
                if(bits < 64) bits = 64;
                // 向上调整到整字节的倍数
                size_t bytes = (bits + 7) / 8;
                bits = bytes * 8;

                // 为filter分配空间，并将其初始化为0
                const size_t init_size = dst->size();
                dst->resize(init_size + bytes, 0);
                // 将hash函数的个数k_压入最后一个字节位
                dst->push_back(static_cast<char>(k_));
                char* array = &(*dst)[init_size];
                // 依次计算n个keys的hash
                for(int i=0; i<n; i++) {
                    // Use double-hashing to generate a sequence of hash values.
                    // See analysis in [Kirsch,Mitzenmacher 2006].
                    uint32_t h = BloomHash(keys[i]);
                    // Rotate right 17 bits
                    const uint32_t delta = (h >> 17) | (h << 15);
                    // 计算k_次哈希
                    for(size_t j=0; j<k_; j++) {
                        // 计算bit位置
                        const uint32_t bitpos = h % bits; 
                        // 将bitpos位置的bit位设置为1
                        array[bitpos / 8] |= (1 << (bitpos % 8));
                        h += delta;
                    }
                }
            }
            // 使用布隆过滤器
            bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override {
                const size_t len = bloom_filter.size();
                if(len < 2) return false;

                const char* array = bloom_filter.data();
                // 注意这里使用的是len-1，这是因为最后一个字节存的是布隆过滤器使用的哈希函数的数量，也即k_
                const size_t bits = (len-1) * 8;
                // 获取bloom_filter使用的哈希函数数量，也即k_值
                const size_t k = array[len - 1];

                if(k > 30) {
                    // Reserved for potentially new encodings for short bloom filters.
                    // Consider it a match.
                    return true;
                }
                // 用相同的哈希方式来进行验证
                uint32_t h = BloomHash(key);
                const uint32_t delta = (h >> 17) | (h << 15);
                for(size_t j=0; j<k;j ++) {
                    const uint32_t bitpos = h % bits;
                    if((array[bitpos/8] & (1 << (bitpos % 8))) == 0)
                        return false;
                    h += delta;
                }
                return true;
            }


            private:
            // 每个key占用的bit
            size_t bits_per_key_; 
            // 需要计算k_个hash值(也即需要k_个hash函数)
            size_t k_;
        };
    }// end anoymous namespace

    const  FilterPolicy* NewBloomFilterPolicy(int bits_per_key) {
        return new BloomFilterPolicy(bits_per_key);
    }
} // end namespace leveldb 