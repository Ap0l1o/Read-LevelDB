//
// Created by Ap0l1o on 2022/3/26.
//

#ifndef LLEVELDB_THREAD_ANNOTATIONS_H
#define LLEVELDB_THREAD_ANNOTATIONS_H

// Use Clang's thread safety analysis annotations when available. In other
// environments, the macros receive empty definitions.
// Usage documentation: https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
//
// 线程安全注解，其基本思路是，通过代码注解（Annotations）告诉编译器那些成员变量和成员函数是受哪个mutex保护，
// 这样如果忘了加锁，编译器会给出警告。
// 用户只需要在代码中用GUARDED_BY表明哪个成员变量是被哪个mutex保护的，就可以让clang帮助检查有没有遗漏加锁的
// 情况。
//
// 下面使用的是Clang线程安全分析中定义的属性，Clang Thread Safety Analysis是C++语言扩展，它
// 警告代码中潜在的竞争条件，分析是完全静态的（即编译时），没有运行时开销。

#if !defined(THREAD_ANNOTATION_ATTRIBUTE__)

#if defined(__clang__)

#define THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#else
#define THREAD_ANNOTATION_ATTRIBUTE__(x)  // no-op
#endif

#endif  // !defined(THREAD_ANNOTATION_ATTRIBUTE__)

#ifndef GUARDED_BY
#define GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))
#endif

#ifndef PT_GUARDED_BY
#define PT_GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))
#endif

#ifndef ACQUIRED_AFTER
#define ACQUIRED_AFTER(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(acquired_after(__VA_ARGS__))
#endif

#ifndef ACQUIRED_BEFORE
#define ACQUIRED_BEFORE(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(acquired_before(__VA_ARGS__))
#endif

#ifndef EXCLUSIVE_LOCKS_REQUIRED
#define EXCLUSIVE_LOCKS_REQUIRED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_locks_required(__VA_ARGS__))
#endif

#ifndef SHARED_LOCKS_REQUIRED
#define SHARED_LOCKS_REQUIRED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_locks_required(__VA_ARGS__))
#endif

#ifndef LOCKS_EXCLUDED
#define LOCKS_EXCLUDED(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))
#endif

#ifndef LOCK_RETURNED
#define LOCK_RETURNED(x) THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))
#endif

#ifndef LOCKABLE
#define LOCKABLE THREAD_ANNOTATION_ATTRIBUTE__(lockable)
#endif

#ifndef SCOPED_LOCKABLE
#define SCOPED_LOCKABLE THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)
#endif

#ifndef EXCLUSIVE_LOCK_FUNCTION
#define EXCLUSIVE_LOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_lock_function(__VA_ARGS__))
#endif

#ifndef SHARED_LOCK_FUNCTION
#define SHARED_LOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_lock_function(__VA_ARGS__))
#endif

#ifndef EXCLUSIVE_TRYLOCK_FUNCTION
#define EXCLUSIVE_TRYLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(exclusive_trylock_function(__VA_ARGS__))
#endif

#ifndef SHARED_TRYLOCK_FUNCTION
#define SHARED_TRYLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(shared_trylock_function(__VA_ARGS__))
#endif

#ifndef UNLOCK_FUNCTION
#define UNLOCK_FUNCTION(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(unlock_function(__VA_ARGS__))
#endif

#ifndef NO_THREAD_SAFETY_ANALYSIS
#define NO_THREAD_SAFETY_ANALYSIS \
  THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)
#endif

#ifndef ASSERT_EXCLUSIVE_LOCK
#define ASSERT_EXCLUSIVE_LOCK(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(assert_exclusive_lock(__VA_ARGS__))
#endif

#ifndef ASSERT_SHARED_LOCK
#define ASSERT_SHARED_LOCK(...) \
  THREAD_ANNOTATION_ATTRIBUTE__(assert_shared_lock(__VA_ARGS__))
#endif


#endif //LLEVELDB_THREAD_ANNOTATIONS_H
