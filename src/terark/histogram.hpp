#ifndef __terark_histogram_hpp__
#define __terark_histogram_hpp__
#pragma once

#include "gold_hash_map.hpp"
#include "valvec.hpp"

namespace terark {

template<class index_t>
class TERARK_DLL_EXPORT Histogram {
    terark::valvec<index_t> m_small_cnt;
    terark::valvec<std::pair<index_t, index_t> > m_large_cnt_compact;
    terark::gold_hash_map<index_t, index_t> m_large_cnt;
    const size_t m_max_small_value;

public:
    size_t m_distinct_key_cnt;
    size_t m_cnt_sum;
    size_t m_total_key_len;
    size_t m_min_key_len;
    size_t m_max_key_len;
    index_t m_min_cnt_key;
    index_t m_max_cnt_key;
    size_t m_cnt_of_min_cnt_key;
    size_t m_cnt_of_max_cnt_key;

    ~Histogram();
    Histogram(size_t max_small_value);
    Histogram();
    index_t& operator[](size_t val) {
        if (val < m_max_small_value)
            return m_small_cnt[val];
        else
            return m_large_cnt[val];
    }
    void finish();

    template<class OP>
    void for_each(OP op) const {
        const index_t* pCnt = m_small_cnt.data();
        for (size_t key = 0, maxKey = m_small_cnt.size(); key < maxKey; ++key) {
            if (pCnt[key]) {
                op(index_t(key), pCnt[key]);
            }
        }
        for (auto kv : m_large_cnt_compact) {
            op(kv.first, kv.second);
        }
    }
};

typedef Histogram<uint32_t> Uint32Histogram;
typedef Histogram<uint64_t> Uint64Histogram;

} // namespace terark

#endif // __terark_histogram_hpp__
