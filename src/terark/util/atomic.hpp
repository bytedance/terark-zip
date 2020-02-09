#pragma once
#include <atomic>

namespace terark {

    template<class T>
    inline
    std::atomic<T>& as_atomic(T& x) {
        return reinterpret_cast<std::atomic<T>&>(x);
    }
    template<class T>
    inline
    const std::atomic<T>& as_atomic(const T& x) {
        return reinterpret_cast<const std::atomic<T>&>(x);
    }
    template<class T>
    inline
    volatile std::atomic<T>& as_atomic(volatile T& x) {
        return reinterpret_cast<volatile std::atomic<T>&>(x);
    }

// cax_* are more powerful
// cas_* are less powerful

#if  defined(__GNUC__) && \
    !defined(__clang__) && \
  ( \
	defined(__i386__) || defined(__i386) || defined(_M_IX86) || \
	defined(__X86__) || defined(_X86_) || \
	defined(__THW_INTEL__) || defined(__I86__) || \
	defined(__amd64__) || defined(__amd64) || \
    defined(__x86_64__) || defined(__x86_64) || defined(_M_X64) \
  )
// only for gnu gcc
// because gnu gcc atomic<> will not generate cmpxchg16b for x86
// see https://gcc.gnu.org/bugzilla/show_bug.cgi?id=80878

    template<size_t> struct GccAtomicType;
    template<> struct GccAtomicType< 1> { typedef char  type; };
    template<> struct GccAtomicType< 2> { typedef short type; };
    template<> struct GccAtomicType< 4> { typedef int   type; };
    template<> struct GccAtomicType< 8> { typedef long long type; };
    template<> struct GccAtomicType<16> { typedef __int128 type; };

  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wstrict-aliasing"
    template<class T>
    inline bool cas_weak(T& x, T expected, T desired) {
        typedef typename GccAtomicType<sizeof(T)>::type A;
        return __sync_bool_compare_and_swap((A*)&x, *(A*)&expected, *(A*)&desired);
    }
    template<class T>
    inline bool cas_weak(volatile T& x, T expected, T desired) {
        typedef typename GccAtomicType<sizeof(T)>::type A;
        return __sync_bool_compare_and_swap((A*)&x, *(A*)&expected, *(A*)&desired);
    }
    template<class T>
    inline bool cas_strong(T& x, T expected, T desired) {
        typedef typename GccAtomicType<sizeof(T)>::type A;
        return __sync_bool_compare_and_swap((A*)&x, *(A*)&expected, *(A*)&desired);
    }
    template<class T>
    inline bool cas_strong(volatile T& x, T expected, T desired) {
        typedef typename GccAtomicType<sizeof(T)>::type A;
        return __sync_bool_compare_and_swap((A*)&x, *(A*)&expected, *(A*)&desired);
    }
  #pragma GCC diagnostic pop
#else
    template<class T>
    inline
    bool cas_weak(T& x, T expected, T desired) {
        return reinterpret_cast<std::atomic<T>&>(x)
        .compare_exchange_weak(expected, desired,
            std::memory_order_release, std::memory_order_relaxed);
    }
    template<class T>
    inline
    bool cas_weak(volatile T& x, T expected, T desired) {
        return reinterpret_cast<volatile std::atomic<T>&>(x)
        .compare_exchange_weak(expected, desired,
            std::memory_order_release, std::memory_order_relaxed);
    }

    template<class T>
    inline
    bool cas_strong(T& x, T expected, T desired) {
        return reinterpret_cast<std::atomic<T>&>(x)
        .compare_exchange_strong(expected, desired,
            std::memory_order_release, std::memory_order_relaxed);
    }
    template<class T>
    inline
    bool cas_strong(volatile T& x, T expected, T desired) {
        return reinterpret_cast<volatile std::atomic<T>&>(x)
        .compare_exchange_strong(expected, desired,
            std::memory_order_release, std::memory_order_relaxed);
    }
#endif

    template<class T>
    inline
    bool cax_weak(T& x, T& expected, T desired) {
        return reinterpret_cast<std::atomic<T>&>(x)
        .compare_exchange_weak(expected, desired,
            std::memory_order_release, std::memory_order_relaxed);
    }
    template<class T>
    inline
    bool cax_weak(volatile T& x, T& expected, T desired) {
        return reinterpret_cast<volatile std::atomic<T>&>(x)
        .compare_exchange_weak(expected, desired,
            std::memory_order_release, std::memory_order_relaxed);
    }

    template<class T>
    inline
    bool cax_strong(T& x, T& expected, T desired) {
        return reinterpret_cast<std::atomic<T>&>(x)
        .compare_exchange_strong(expected, desired,
            std::memory_order_release, std::memory_order_relaxed);
    }
    template<class T>
    inline
    bool cax_strong(volatile T& x, T& expected, T desired) {
        return reinterpret_cast<volatile std::atomic<T>&>(x)
        .compare_exchange_strong(expected, desired,
            std::memory_order_release, std::memory_order_relaxed);
    }

    template<class T>
    inline
    T atomic_maximize(T& x, T y,
                      std::memory_order morder = std::memory_order_relaxed) {
        T x0 = as_atomic(x).load(morder);
        T zz = x0 < y ? y : x0;
        while (!as_atomic(x).compare_exchange_weak(x0, zz, morder, morder)) {
            zz = x0 < y ? y : x0;
        }
        return zz;
    }

    template<class T>
    inline
    T atomic_minimize(T& x, T y,
                      std::memory_order morder = std::memory_order_relaxed) {
        T x0 = as_atomic(x).load(morder);
        T zz = x0 < y ? x0 : y;
        while (!as_atomic(x).compare_exchange_weak(x0, zz, morder, morder)) {
            zz = x0 < y ? x0 : y;
        }
        return zz;
    }

/*
    enum class LockType {
        LockNone,
        LockFree,
        LockMutex,
    };
*/

}

