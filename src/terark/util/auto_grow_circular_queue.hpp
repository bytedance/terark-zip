#pragma once
#include <terark/util/autofree.hpp>
#include <terark/bitmanip.hpp>
#include <terark/util/throw.hpp>
#include <type_traits>

namespace terark {

template<class T>
class AutoGrowCircularQueue {
    static_assert(std::is_trivially_destructible<T>::value, "T must be trivially destructible");
    AutoFree<T> m_vec;
    size_t      m_cap;
    size_t      m_head;
    size_t      m_tail;
public:
    explicit AutoGrowCircularQueue(size_t cap = 256) {
        if ((cap & (cap-1)) != 0 || 0 == cap) {
            THROW_STD(logic_error, "invalid cap = %zd, must be power of 2", cap);
        }
        m_vec.alloc(cap);
        m_cap = cap;
        m_head = 0;
        m_tail = 0;
    }
    AutoGrowCircularQueue(const AutoGrowCircularQueue& y) {
        m_vec.alloc(y.m_cap);
        m_cap  = y.m_cap ;
        m_head = y.m_head;
        m_tail = y.m_tail;
        memcpy(m_vec.p, y.m_vec.p, sizeof(T) * m_cap);
    }
    AutoGrowCircularQueue& operator=(const AutoGrowCircularQueue& y) {
        if (&y != this) {
            this->~AutoGrowCircularQueue();
            new(this)AutoGrowCircularQueue(y);
        }
        return *this;
    }
    void swap(AutoGrowCircularQueue& y) {
        std::swap(m_vec.p, y.m_vec.p);
        std::swap(m_cap  , y.m_cap  );
        std::swap(m_head , y.m_head );
        std::swap(m_tail , y.m_tail );
    }
    const T& front() const {
        assert(((m_tail - m_head) & (m_cap - 1)) > 0);
        return m_vec.p[m_head];
    }
    T& front() {
        assert(((m_tail - m_head) & (m_cap - 1)) > 0);
        return m_vec.p[m_head];
    }
    void pop_front() {
        assert(((m_tail - m_head) & (m_cap - 1)) > 0);
        m_head = (m_head + 1) & (m_cap - 1);
    }
    T pop_front_val() {
        assert(((m_tail - m_head) & (m_cap - 1)) > 0);
        T x = m_vec.p[m_head];
        m_head = (m_head + 1) & (m_cap - 1);
        return x;
    }
    // caller ensure @param output has enough space
    void pop_all(T* output) {
        T*     base = m_vec.p;
        size_t head = m_head;
        size_t tail = m_tail;
        if (head < tail) {
            memcpy(output, base + head, sizeof(T)*(tail - head));
        }
        else {
            size_t len1 = m_cap - head;
            size_t len2 = tail;
            memcpy(output, base + head, sizeof(T)*len1);
            memcpy(output + len1, base, sizeof(T)*len2);
        }
        m_head = m_tail = 0;
    }
    // caller ensure @param output has enough space
    void pop_n(T* output, size_t n) {
        n = std::min(n, this->size());
        T*     base = m_vec.p;
        size_t head = m_head;
        size_t tail = m_tail;
        if (head < tail) {
            memcpy(output, base + head, sizeof(T)*n);
        }
        else {
            size_t len1 = std::min(m_cap - head, n);
            memcpy(output, base + head, sizeof(T)*len1);
            if (n > len1) {
                size_t len2 = n - len1;
                memcpy(output + len1, base, sizeof(T)*len2);
            }
        }
        m_head = (head + n) & (m_cap - 1);
    }
    terark_forceinline
    void push_back(const T& x) {
        size_t head = m_head;
        size_t tail = m_tail;
        size_t cap1 = m_cap - 1;
        if (terark_likely(((head - tail) & cap1) != 1)) {
            m_vec.p[tail] = x;
            m_tail = (tail + 1) & cap1;
        }
        else { // queue is full
            push_back_slow_path(x);
        }
    }
    size_t capacity() const { return m_cap; }
    size_t size() const { return (m_tail - m_head) & (m_cap - 1); }
    bool  empty() const { return m_head == m_tail; }
private:
    terark_no_inline
    void push_back_slow_path(const T& x) {
        double_cap();
        m_vec.p[m_tail] = x;
        m_tail = (m_tail + 1) & (m_cap - 1);
    }
    void double_cap() {
        size_t cap = m_cap;
        m_vec.resize(cap * 2);
        T*     base = m_vec.p;
        size_t head = m_head;
        if (head > cap / 2) {
            memcpy(base + cap + head, base + head, sizeof(T)*(cap - head));
            m_head = cap + head;
        }
        else {
            size_t tail = m_tail;
            memcpy(base + cap, base, sizeof(T)*(tail));
            m_tail = cap + tail;
        }
        m_cap = cap * 2;
    }
};

}

