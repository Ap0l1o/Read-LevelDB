#include "db/dbformat.h"
#include <cstddef>
#include <cstdio>
#include <sstream>
#include "util/coding.h"
#include "port/port.h"

namespace leveldb {

    // 将序号和类型打包到一块
    static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
        assert(seq < kMaxSequenceNumber);
        assert(t <= kValueTypeForSeek);
        return (seq << 8) | t;
    }

    // 将ParsedInternalKey序列化添加到*result
    void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
        result->append(key.user_key.data(), key.user_key.size());
        PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
    }

    std::string ParsedInternalKey::DebugString() const {
        std::ostringstream ss;
        ss << '\'' << EscapeString(user_key.ToString()) << "' @" << sequence << " : "
           << static_cast<int>(type);
        
        return ss.str();
    }

    std::string InternalKey:: DebugString() const {
        ParsedInternalKey parsed;
        if(ParseInternalKey(rep_, &parsed)) {
            return parsed.DebugString();
        }
        std::ostringstream ss;
        ss << "(bad)" << EscapeString(rep_);
        return ss.str();
    }

    const char* InternalKeyComparator::Name() const {
        return "leveldb.InternalKeyComparator";
    }

    int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
        // Order by:
        //    increasing user key (according to user-supplied comparator) key的升序
        //    decreasing sequence number  序号的降序
        //    decreasing type (though sequence# should be enough to disambiguate) 类型的降序
        // 先对key进行比较
        int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
        // key相同再进一步比较序号和类型
        if(r == 0) {
            // 获取序号
            const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
            const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
            if(anum > bnum) {
                r = -1;
            } else if(anum < bnum) {
                r = +1;
            }
        }
        return r;

    }

    void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                      const Slice& limit) const {
        // 尝试去缩短key的用户部分
        Slice user_start = ExtractUserKey(*start);
        Slice user_limit = ExtractUserKey(limit);
        std::string tmp(user_start.data(), user_start.size());
        user_comparator_->FindShortestSeparator(&tmp, user_limit);
        if(tmp.size() < user_start.size() && 
           user_comparator_->Compare(user_start, tmp) < 0 ) {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            // 从物理上来讲，User key 占用的空间缩小了，但是其逻辑大小还是大的
            PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
            assert(this->Compare(*start, tmp) < 0);
            assert(this->Compare(tmp, limit) < 0);
            // 缩小后与原数据进行交换
            start->swap(tmp);
        }
    }

    void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
        // 提取user key
        Slice user_key = ExtractUserKey(*key);
        // 创建副本
        std::string tmp(user_key.data(), user_key.size());
        user_comparator_->FindShortSuccessor(&tmp);
        if(tmp.size() < user_key.size() &&
           user_comparator_->Compare(user_key, tmp) < 0) {
            // 此时在物理长度上变短了，但逻辑大小还是大的
            PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
            assert(this->Compare(*key, tmp) < 0);
            key->swap(tmp);
        }
    }

    const char* InternalFilterPolicy::Name() const { return user_policy_->Name(); }

    void InternalFilterPolicy::CreateFilter(const Slice* keys, int n, std::string* dst) const {
        // 传入的keys是internal key，需要先提取出user key
        Slice* mkey = const_cast<Slice*>(keys);
        for(int i=0; i<n; i++) {
            mkey[i] = ExtractUserKey(keys[i]);
        }
        user_policy_->CreateFilter(keys, n, dst);
    }

    bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
        // 传入的key为internal key， 需要提取user key再进行匹配
        return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
    }

    LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
        // 获取user key的长度
        size_t usize = user_key.size();
        // 预估需用空间大小：length(4) + 数据 + tag(8)
        size_t needed = usize + 13;
        char* dst;
        if(needed <= sizeof(space_)) {
            dst = space_;
        } else {
            dst = new char[needed];
        }
        // start_指向头指针
        start_ = dst;
        // 存入数据长度 = user key + tag， 返回值为尾指针
        dst = EncodeVarint32(dst, usize + 8);
        // kstart_指向数据头
        kstart_ = dst;
        // 存入user key
        std::memcmp(dst, user_key.data(), usize);
        // 存入 tag = seq + type
        dst += usize;
        EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
        dst += 8;
        // end_ 指向尾指针
        end_ = dst;
    }
} // end namespace leveldb