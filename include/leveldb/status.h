// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
// 状态封装了操作的结果。 它可能指示成功，也可能指示错误并带有相关的错误消息。
// 
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

// LevelDB中的返回状态，将错误号和错误信息封装称Status类，统一进行处理

#ifndef STATUS_H_
#define STATUS_H_
#include<algorithm>
#include<string>

#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {
    class LEVELDB_EXPORT Status {
        public:
        // 创建一个成功状态（空指针表示成功）
        Status() noexcept : state_(nullptr) {}
        ~Status() { delete[] state_; } // state_是用new创建的数组，所以在析构函数中使用delete来释放

        Status(const Status& rhs);
        Status& operator=(const Status& rhs);

        // 这里的「&&」是右值引用，可以避免复制开销（右值引用产生临时量来存储常量，利用右值的构造来减少对象构造和析构操作以达到提高效率的目的）
        // 注：单个「&」是左值引用，有名称的、可以获取到存储地址的表达式即为左值，反之则是右值
        Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; } 
        Status& operator=(Status&& rhs) noexcept;

        // 返回一个成功状态
        static Status OK() { return Status(); }

        // =======================================  根据相关msg构造各种状态  ===============================================
        static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
            return Status(kNotFound, msg, msg2);
        }
        static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
            return Status(kCorruption, msg, msg2);
        }
        static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
            return Status(kNotSupported, msg, msg2);
        }
        static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
            return Status(kInvalidArgument, msg, msg2);
        }
        static Status IOError(const Slice& msg, const Slice& msg2) {
            return Status(kIOError, msg, msg2);
        }

        // =========================================  判断当前状态码  =====================================================
        bool ok() const { return (state_ == nullptr); }
        bool IsNotFound() const { return (code() == kNotFound); }
        bool IsCorruption() const { return (code() == kCorruption); }
        bool IsNotSupported() const { return (code() == kNotSupported); }
        bool IsInvalidArgument() const { return (code() == kInvalidArgument); }
        bool IsIOError() const { return (code() == kIOError); }

        // 将该status转为适合打印的string形式
        // 成功时返回 string "OK"
        std::string ToString() const;

        private:
        enum Code {
            kOk = 0,
            kNotFound = 1,
            kCorruption = 2,
            kNotSupported = 3,
            kInvalidArgument = 4,
            kIOError = 5
        };

        // 返回状态，状态为空指针（也即成功状态）返回KOk（就是ok的意思），否则返回相应的状态返回码code
        Code code() const {
            return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
        }

        Status(Code code, const Slice& msg, const Slice& msg2);
        static const char* CopyState(const char* s);

        // state_为空指针表示成功状态Ok，否则state_是一个用new创建的包含如下信息的字节数组
        // state_[0]~state_[3] 存储消息message长度
        // state_[4] 存储消息code
        // state_[5]~ 存储消息message
        const char* state_;

    };

    // ======================================  部分函数实现，均为内敛函数  ================================================
    inline Status::Status(const Status& rhs) {
        state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
    }
    
    inline Status& Status::operator=(const Status& rhs) {
        // 以下会考虑 this == &rhs 的情况，以及两者均为ok的情况
        if( state_ != rhs.state_ ) {
            delete[] state_; // 释放之前的空间
            state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
        }

        return *this;
    }

    inline Status& Status::operator=(Status&& rhs) noexcept {
        std::swap(state_, rhs.state_);
        return *this;
    }
} // namespace leveldb



#endif // STATUS_H_