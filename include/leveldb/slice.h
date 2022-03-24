/**
 * @file slice.h
 * @author XingFei Yao (your.x.free@gmail.com)
 * @brief read the open source code of leveldb
 * @version 0.1
 * @date 2021-12-17
 * 
 * @copyright Copyright (c) 2021
 * 
 */

/**
 * 在同一个文件中只能将同一个「头文件」包含一次。
 * 有一种标准的C/C++技术可以避免多次包含同一个「头文件」，它是基于预处理器编译指令「#ifndef」（即 if not define）的。
 * 下面的代码意味着 仅当以前没有使用预处理器编译指令「#define」定义名称「SLICE_」时，才处理「#ifndef」和「#endif」之间的代码。
 * 编译器首次遇到该文件时，名称「SLICE_」还没有定义（我们根据include文件名来选择名称，并加上一些下划线，以创建一个在其他地方不太可能被定义的名称）。
 * 在这种情况下，编译器将查看「#ifndef」和「#endif」之间的内容，并读取定义「SLICE_」的一行。
 * 如果在同一个文件中遇到其他包含「slice.h」的代码，编译器将知道「SLICE_」已经定义了，从而跳到「#endif」后面的一行上。
 * 注意：这种方法并不能防止编译器将文件包含两次，而只是让它忽略除第一次包含之外的所有内容。
 *      大多数标准C和C++头文件都使用这种防护方案，否则，可能在一个文件中定义同一个结构两次，这会导致编译错误。
 */
#ifndef SLICE_H_
#define SLICE_H_

#include<cassert>
#include<cstddef>
#include<cstring>
#include<string>
/**
 * @brief Slice is a simple structure containing a pointer into some external 
 * storage and a size.  The user of a Slice must ensure that the slice 
 * is not used after the corresponding external storage has been deallocated.
 * Slice是一个简单的数据结构，包含了一个指向外部字节数组的指针，以及字节数组的长度
 */
namespace leveldb {
    class Slice {
    public:
        /* Slice各种形式的构造函数 */

        // 默认构造函数，为所有数据成员提供隐式的初始值————创建一个空的Slice
        Slice() : data_(""), size_(0) {};
        // 创建一个引用C风格字符串d[0,n-1]的Slice
        Slice(const char* d, size_t n) : data_(d), size_(n) {};
        // 创建一个引用s的内容的Slice，s是C++的字符串（string）类型
        Slice(const std::string& s) : data_(s.data()), size_(s.size()) {};
        // 创建一个引用d[0, strlen(d)-1]的字符串，d是C风格字符串
        Slice(const char* d) : data_(d), size_(strlen(d)) {};
        // 使用一个Slice对象来创建新的Slice
        Slice(const Slice&) = default;

        Slice& operator=(const Slice&) = default;

        // 返回引用外部字节数组的指针，指针指向数组的首部
        // 注：除了构造函数之外的其他函数都使用了「const」来防止其修改「调用对象」，
        //    只要类方法不修改调用对象，就应该将其声明为「const」。
        const char* data() const { return data_; }

        // 返回字节数组的长度，单位是字节
        size_t size() const { return size_; }

        // 返回字符串是否为空，也即长度size_是否为0
        bool empty() const { return size_ == 0;}

        // 重载运算符"[]"，使其可以按照数组的形式进行访问
        char operator[](size_t n) const {
            assert(n < size_); // 使用「断言」判断一下是否越界，越界则终止程序
            return data_[n];
        }

        // 清空字符串
        void clear() {
            data_ = "";
            size_ = 0;
        }

        // 丢掉slice的前n个字节
        void remove_prefix(size_t n) {
            assert(n <= size()); // 使用「断言」判断一下是否越界，越界则终止程序
            data_ += n;
            size_ -= n;
        }

        // 返回一个string， 该string复制了data_指针指向的字符串的内容
        std::string ToString() const { return std::string(data_, size_); }

        // Slice之间进行比较
        int compare(const Slice& b) const;

        // 判断Slice x是否是当前Slice的前缀
        bool starts_with(const Slice& x) const {
            // 比较，当前slice长度不能比x长度小，且x确实是当前Slice的前缀
            // 问题：size_应该是private，不能直接访问，但是当前版本源码是直接访问的
            // return ( (size_ >= x.size_) && (memcmp(data_, x.data_, x.size_)) ); 
            return ( (size_ >= x.size()) && (memcmp(data_, x.data(), x.size()) == 0 ) ); 
        }


    private:
        /**
         * 构造函数的参数名不能与类成员相同，为避免同名混乱，
         * 通常在数据成员名中加前缀「m_」或后缀「_」，
         * 可以看到Slice类的两个数据成员都使用了「_」后缀。
         */
        const char* data_; // 指向外部字节数组的指针
        size_t size_; // 字节数组的长度

    };

    /**
     * 关于内联函数：常规的函数调用会使程序跳到另一个地址（函数的地址），然后在函数执行完毕后再跳回来，
     *             这会带来一定的开销。C++内联函数提供了另一种选择。对于内敛函数，编译器将使用相应的
     *             函数代码替换函数调用。因此，对于内敛函数，程序无需跳到另一个位置执行代码然后再跳回。
     *             因此，内敛函数的运行速度比常规函数较快，但代价是需要占用更多的内存。应该有选择的使用
     *             内敛函数：
     *                      - 如果执行函数代码的时间比处理函数调用机制的时间长，则将生的时间只占整个过程的
     *                        一小部分，此时没必要使用内敛函数；
     *                      - 如果代码执行时间很短，则内敛调用就可以节省非内敛调用使用的大部分时间；
     *             注：其「定义」位于类声明中的函数都将自动成为内敛函数，类声明常将短小的成员函数作为内敛函数
     *                如果愿意，也可以在类声明之外定义成员函数，并使其成为内敛函数，只需要在类实现部分中定义
     *                函数实现时使用「inline」限定符即可。
     * 
     * 关于按引用传递参数：可以看到，大多数方法都采用了按引用传递的参数的方法，这里是出于效率的考虑，
     *                  按引用传递参数只会传递地址，按值传递需要创建副本，占内存且费时间
     * 
     * 关于运算符重载:  1. 对于「类成员重载运算符函数」来说，运算符表达式左边的操作数对应于调用对象（通过this指针传递），
     *                   运算符表达式右边的操作数对应于运算符函数的唯一参数；
     *                2. 对于「非类成员重载运算符函数」来说，运算符表达式左边的操作数对应运算符函数的第一个参数，
     *                   运算符表达式右边的操作数对应函数的第二个参数；
     *                注：非成员版本的重载运算符函数所需的形参数目与运算符使用的操作数数目一样；而成员版本所需的参数数目
     *                    少一个，因为其中一个操作数是被隐式地传递的调用对象（通过this指针的形式传递）。
     *                    且，非成员版本的重载运算符函数应该是「友元函数」，这样它才能直接访问类的「私有数据」。
     */
    // 重载「相等」判断运算符
    inline bool operator==(const Slice& x, const Slice& y) {
        return ( (x.size() == y.size()) && (memcmp(x.data(), y.data(), x.size()) == 0) );
    }

    // 重载「不想等」运算符
    inline bool operator!=(const Slice& x, const Slice& y ) {
        return !(x == y);
    }

    // 定义compare函数
    inline int Slice::compare(const Slice& b) const {
        const size_t min_len = (size_ < b.size()) ? size_ : b.size(); // 获取两个Slice的最小长度（字节）
        int r = memcmp(data_, b.data(), min_len); // 比较前min_len个字节，相等返回0，小于返回负数，大于返回正数
        // 如果前min_len个字节都相等，则进一步比较后面是否还有字节
        if(r == 0) {
            if(size_ < b.size())
                r = -1;
            else if(size_ > b.size())
                r = +1;
        }
        return r;
    }

}// end namespace leveldb

#endif 
// 7   6  5  4  3 2 1 0
// 128 64 32 16 8 4 2 1
// 1 00101100
// 256 + 32 + 8 + 4 = 300
// 原始二进制：   1  00101100
// 编码后二进制： 10 10101100
// 低              高
// 10101100 00000010 