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

    template<class T>
    inline
    bool cas_weak(T& x, T& expected, T desired) {
        return reinterpret_cast<std::atomic<T>&>(x)
        .compare_exchange_weak(expected, desired,
            std::memory_order_release, std::memory_order_relaxed);
    }
    template<class T>
    inline
    bool cas_weak(volatile T& x, T& expected, T desired) {
        return reinterpret_cast<volatile std::atomic<T>&>(x)
        .compare_exchange_weak(expected, desired,
            std::memory_order_release, std::memory_order_relaxed);
    }

    template<class T>
    inline
    bool cas_strong(T& x, T& expected, T desired) {
        return reinterpret_cast<std::atomic<T>&>(x)
        .compare_exchange_strong(expected, desired,
            std::memory_order_release, std::memory_order_relaxed);
    }
    template<class T>
    inline
    bool cas_strong(volatile T& x, T& expected, T desired) {
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

