#ifndef _SKIPLIST_H_
#define _SKIPLIST_H

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {
    // Arena类的前置声明，和函数一样，类也可先声明后定义。
    // 声明之后定义之前的类是一个不完全类型，此时，我们仅知道这是一个类，而不知道它包含哪些类成员。
    // 不完全类型仅能在非常有限的情况下使用： 1. 只能定义指向不完全类型的指针或引用；
    //                                  2. 只能定义以不完全类型为参数或作为返回值的函数；
    class Arena;

    template <typename Key, class Comparator>
    class SkipList {
        private:
        // Node 类的前置声明
        // 注：struct和class都可以用于定义类，不同之处在于struct成员默认是public的，
        //     而class的成员默认是private的。
        struct Node;

        public:
        explicit SkipList(Comparator cmp, Arena* arena);
        SkipList(const SkipList&) = delete;

        // 插入一个key
        void Insert(const Key& key);
        // 检查是否包含指定的key
        bool Contains(const Key& key) const;

        class Iterator {
            public:
            // 构造函数，禁止隐式转换
            // 注：explicit关键字用于修饰仅有一个参数的构造函数，表明该构造函数是显示的而非隐式的
            explicit Iterator(const SkipList* list);

            // 检查该迭代器是否指向一个有效的节点
            bool Valid() const;
            
            // 若当前节点有效则返回当前节点的key
            const Key& key() const;

            // 向后移动一个位置
            void Next();
            
            // 迭代器向前移动一个位置
            void Prev();

            // 向前移动到满足大于目标键的第一个key
            void Seek(const Key& target);

            // 移动到第一个节点
            void SeekToFirst();

            // 移动到最后一个节点
            void SeekToLast();
            
            private:
            const SkipList* list_;
            Node* node_;
        };

        private:
        // 枚举类型，定义跳表允许的最大高度
        enum { kMaxHeight = 12 };

        // 一经构造变不可修改的成员
        Comparator const compare_;
        Arena* const arena_; // 内存池，用于为节点分配空间

        // 节点头
        Node* const head_; 
        // 当前整个跳表的高度，也即当前最高层数
        std::atomic<int> max_height_;

        Random rnd_;

        // 显示定义内敛函数
        // 返回跳表的长度
        inline int GetMaxHeight() const {
            return max_height_.load(std::memory_order_relaxed);
        }

        Node* NewNode(const Key& key, int height);
        int RandomHeight();

        // 判断两个key是否相等
        bool Equal(const Key& a, const Key& b) const {
            return (compare_(a, b) == 0);
        }
        
        // 检查该key是否比节点n的key大
        // 如果key比节点n的key大，则返回true
        bool KeyIsAfterNode(const Key& key, Node* n) const ;

        // 找到第一个满足大于等于该key的节点
        // 找不到则返回nullptr
        //
        // 如果prev非空的话，将每一层的前驱节点存入prev
        // If prev is non-null, fills prev[level] with pointer to previous
        // node at "level" for every level in [0..max_height_-1].
        Node* FindGreaterOrEqual(const Key& key, Node** prev) const ;
        // 找到最后一个小于该key的节点
        // 找不到则返回head_
        Node* FindLessThan(const Key& key) const ;
        // 找到最后一个节点
        // 如果为空链表则返回head_
        Node* FindLast() const ;


    }; // end class SkipList


    // ===============================================           相关实现         ================================================
    // ======================       Node类的实现       ======================     
    // 注：这里使用struct来定义，默认所有成员的访问权限均为public
    template <typename Key, class Comparator>
    struct SkipList<Key, Comparator>::Node {

        Key const key;
        // 禁止隐式转换的构造函数
        explicit Node(const Key& k): key(k) {}

        // 取第n个节点
        Node* Next(int n) {
            assert(n >= 0);
            return next_[n].load(std::memory_order_acquire);
        }
        // 设置第n个节点
        void SetNext(int n, Node* x) {
            assert(n >= 0);
            next_[n].store(x, std::memory_order_release);
        }

        // No-barrier variants that can be safely used in a few locations.
        Node* NoBarrier_Next(int n) {
            assert(n >= 0);
            return next_[n].load(std::memory_order_relaxed);
        }
        void NoBarrier_SetNext(int n, Node* x) {
            assert(n >= 0);
            next_[n].store(x, std::memory_order_relaxed);
        }

        
        private:
        // 数组的长度与跳表当前的高度相同，存储的是节点所在当前层的下一个节点
        std::atomic<Node*> next_[1];
    }; // end struct Node

    // 新建节点：传入Node的key值和树的高度
    template <typename Key, class Comparator>
    typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
        const Key& key, int height) {
        // 分配空间：Node数据结构的大小 + 指向下一节点的数组的长度（因为需要指向下一层节点，所以需要height-1）
        char* const node_memory = arena_->AllocateAligned(
            sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1)
        );
        return new (node_memory)Node(key);
    }

    // ======================  SkipList内部Iterator工具类的内部实现  ============================
    template <typename Key, class Comparator>
    inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
        list_ = list;
        node_ = nullptr;
    }


    template <typename Key, class Comparator>
    inline bool SkipList<Key, Comparator>::Iterator::Valid() const{
        return node_ != nullptr;
    }


    template <typename Key, class Comparator>
    inline const Key& SkipList<Key, Comparator>::Iterator::key() const{
        assert(Valid());
        return node_->key;
    }


    template <typename Key, class Comparator>
    inline void SkipList<Key, Comparator>::Iterator::Next() {
        assert(Valid());
        node_ = node_->Next(0);
    }


    template <typename Key, class Comparator> 
    inline void SkipList<Key, Comparator>::Iterator::Prev() {
        assert(Valid());
        node_ = list_->FindLessThan(node_->key);
        if(node_ == list_->head_) {
            node_ =  nullptr;
        }
    }


    template <typename Key, class Comparator>
    inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
        node_ = list_->FindGreaterOrEqual(target, nullptr);
    } 


    template <typename Key, class Comparator>
    inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
        node_ = list_->head_->Next(0);
    }


    template <typename Key, class Comparator>
    inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
        node_ = list_->FindLast();
        if(node_ == list_->head_) {
            node_ = nullptr;
        }
    }


    // ===========================================   SkipList内部具体实现   ========================================
    // 生成一个随机高度也即层数，即该节点存放的最高层数
    template <typename Key, class Comparator>
    int SkipList<Key, Comparator>::RandomHeight() {
        static const unsigned int kBranching = 4;
        int height = 1;
        // 为了避免节点均匀分配到每一层
        // 将每一层的节点按照4的倍数减少，以提高查询效率
        while(height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
            height++;
        }
        assert(height > 0);
        assert(height <= kMaxHeight);
        return height;
    }

    template <typename Key, class Comparator>
    bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, SkipList::Node* n) const {
        // 判断key是否比节点n的key大
        return (n != nullptr) && (compare_(n->key, key) < 0);
    }

    // 找到key在每一层的前驱节点并将其存在prev中，最后返回在第一层的前驱节点
    template <typename Key, class Comparator> 
    typename SkipList<Key, Comparator>::Node*
    SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, SkipList::Node** prev) const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        while(true) {
            // 从跳表的最上层开始查找
            Node* next = x->next_[level];
            // 若key大于next的key，则需要继续在当前层查找
            if(KeyIsAfterNode(key, next)) {
                x = next;
            } else {
                // next的key值已经大于key，所以需要往下层找

                if(prev != nullptr) {
                    // 保存每一层大于key的前驱节点
                    prev[level] = next;
                }

                if(level == 0) {
                    // 如果已经找到第一层了，则返回第一层的前驱节点
                    return next;
                } else {
                    // 当前不是第一层，需要继续去下层查找
                    level--;
                }
            }
        }
    }

    template <typename Key, class Comparator>
    typename SkipList<Key, Comparator>::Node*
    SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
        // 找到最后一个小于key的节点并返回，找不到返回头节点head_
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        while(true) {
            // 若空跳表或当前头节点已经比key小，则没有继续查找的必要了
            assert(x == nullptr || compare_(x->key, key) < 0);
            // 依旧是从跳表的最上层开始查找
            Node* next = x->Next(level);
            // 若当前节点为空 或 当前节点已经大于key，则需要去下一层查找
            if(next == nullptr || compare_(next->key, key) >= 0) { 
                // 已经在第一层，没有下一层了，则等于没找到比他小的，
                // 直接返回头节点 
                if(level == 0) {
                    return x;
                } else {
                    level--;
                }
            } else {
                // 当前节点仍小于key，需要在当前层继续查找
                x = next;
            }
        }
    }

    template <typename Key, class Comparator>
    typename SkipList<Key, Comparator>::Node* 
    SkipList<Key, Comparator>::FindLast() const{
        // 查找最后一个节点并返回
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        // 依旧是从最高层开始查找
        while(true) {
            Node* next = x->Next(level);
            // 若当前节点已经为空，则应该往下层查找
            if(next == nullptr) {   
                // 若已经到了第一层则直接返回，否则去下层查找
                if(level == 0) {
                    return x;
                } else {
                    level--;
                }
            } else {
                // 当前节点不为空，则继续在当前层查找
                x = next;
            }
        }
    }

    template <typename Key, class Comparator>
    SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena) 
        : compare_(cmp),
          arena_(arena),
          head_(NewNode(0, kMaxHeight)),
          max_height_(1),
          rnd_(0xdeadbeef) {
        for(int i=0; i<kMaxHeight; i++) {
            // 将头节点在每一层的后继节点全部设置为nullptr
            head_->SetNext(i, nullptr);
        }
    }

    template <typename Key, class Comparator>
    void SkipList<Key, Comparator>::Insert(const Key& key) {
        // 先找到要插入的key在每一层的前驱节点
        Node* prev[kMaxHeight];
        Node* x = FindGreaterOrEqual(key, prev);
        // 不允许在跳表中插入两个一样的key
        assert(x == nullptr || !Equal(key, x->key));
        // 随机生成的高度大于当前高度，初始化其前驱节点为头节点
        int height = RandomHeight();
        if(height > GetMaxHeight()) {
            for(int i=GetMaxHeight(); i<height; i++) {
                prev[i] = head_;
            }
            // It is ok to mutate max_height_ without any synchronization
            // with concurrent readers.  A concurrent reader that observes
            // the new value of max_height_ will see either the old value of
            // new level pointers from head_ (nullptr), or a new value set in
            // the loop below.  In the former case the reader will
            // immediately drop to the next level since nullptr sorts after all
            // keys.  In the latter case the reader will use the new node.
            max_height_.store(height, std::memory_order_relaxed);
        }
        // 创建新节点
        x = NewNode(key, height);
        // 设置其在每一层的前驱和后继节点
        for(int i=0; i<height; i++) {
            //设置其后继节点
            x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next[i]);
            // 将其前驱节点的后继节点设置为当前节点
            prev[i]->SetNext(i, x);
        }
    }

    template <typename Key, class Comparator>
    bool SkipList<Key, Comparator>::Contains(const Key& key) const {
        Node* x = FindGreaterOrEqual(key, nullptr);
        if(x != nullptr || Equal(x->key, key)) {
            return true;
        } else {
            return false;
        }
    }


} // end namespace leveldb;

#endif