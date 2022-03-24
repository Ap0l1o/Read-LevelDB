#include "leveldb/comparator.h"
#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>

#include "leveldb/slice.h"

namespace leveldb {
    Comparator::~Comparator() = default;
    namespace {
        // Comparator的实现类
        class BytewiseComparatorImpl : public Comparator {
            public:
            BytewiseComparatorImpl() = default;
            const char* Name() const override { return "leveldb.BytewiseComparator"; }
            int Compare(const Slice& a, const Slice& b) const override {
                return a.compare(b);
            }

            void FindShortestSeparator(std::string* start, const Slice& limit) const override {
                // 先找公共前缀
                // 两者中较短字符串的长度
                size_t min_length = std::min(start->size(), limit.size());
                size_t diff_index = 0; // 开始不一致时的下标
                // 开始循环找公共前缀
                while( (diff_index < min_length) && ((*start)[diff_index] == limit[diff_index]) ) {
                    diff_index++;
                }
                // diff_index >= min_length 此时较短的字符串是较长字符串的前缀，此时不需要任何操作
                if(diff_index >= min_length) {
                    // no operation
                } else {
                    // 在不超过limit的前提下将start+1
                    uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
                    if(diff_byte < static_cast<uint8_t>(0xff) && 
                       diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
                        (*start)[diff_index]++;
                        // 重设长度
                        start->resize(diff_index+1);
                        assert(Compare(*start, limit) < 0);
                    }
                }
            }

            void FindShortSuccessor(std::string* key) const override {
                // 找到第一个可以增加的字符
                size_t n = key->size();
                for(size_t i=0; i<n; i++) {
                    const uint8_t byte = (*key)[i];
                    // 找到满足条件的字符，将其加1
                    if( byte != static_cast<uint8_t>(0xff)) {
                        (*key)[i] = byte + 1;
                        // 重设长度(也即截断字符串)
                        key->resize(i + 1);
                        return;
                    }
                }
                // 找不到符合条件的字符则不做任何操作
                // *key is a run of 0xffs.  Leave it alone.
            }
        };

    } // anonymous namespace

    const Comparator* BytewiseComparator() {
        // not implement currently
//        static NoDestructor<BytewiseComparatorImpl> singleton;
//        return singleton.get();
    }

} // namespace leveldb