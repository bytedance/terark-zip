#include "histogram.hpp"

namespace terark {

template<class index_t>
Histogram<index_t>::~Histogram() {
}

template<class index_t>
Histogram<index_t>::Histogram(size_t max_small_value)
	: m_max_small_value(max_small_value)
{
    m_distinct_key_cnt = 0;
    m_cnt_sum = 0;
    m_total_key_len = 0;
    m_min_key_len = size_t(-1);
    m_max_key_len = 0;
    m_min_cnt_key = index_t(-1);
    m_max_cnt_key = 0;
    m_cnt_of_min_cnt_key = size_t(-1);
    m_cnt_of_max_cnt_key = 0;
    m_small_cnt.resize(m_max_small_value, 0);
}

template<class index_t>
Histogram<index_t>::Histogram() : Histogram(1024) {
}

template<class index_t>
void Histogram<index_t>::finish() {
    const index_t* pCnt = m_small_cnt.data();
    for (size_t maxKey = m_max_small_value; maxKey > 0; --maxKey) {
        if (pCnt[maxKey - 1] > 0) {
            m_small_cnt.risk_set_size(maxKey);
            break;
        }
    }
    size_t distinct_cnt = 0;
    size_t sum = 0;
    size_t len = 0;
    size_t cnt_of_min_cnt_key = size_t(-1);
    size_t cnt_of_max_cnt_key = 0;
    for (size_t key = 0, maxKey = m_small_cnt.size(); key < maxKey; ++key) {
        index_t cnt = pCnt[key];
        if (cnt) {
            distinct_cnt++;
            sum += cnt;
            len += cnt * key;
            if (cnt_of_min_cnt_key > cnt) {
                cnt_of_min_cnt_key = cnt;
                m_min_cnt_key = key;
            }
            if (cnt_of_max_cnt_key < cnt) {
                cnt_of_max_cnt_key = cnt;
                m_max_cnt_key = key;
            }
            if (m_min_key_len > key) {
              m_min_key_len = key;
            }
            if (m_max_key_len < key) {
              m_max_key_len = key;
            }
        }
    }
    m_large_cnt_compact.resize_no_init(m_large_cnt.size());
    auto large_beg = m_large_cnt_compact.begin();
    auto large_num = m_large_cnt.end_i();
    for (size_t idx = 0; idx < large_num; ++idx) {
        index_t key = m_large_cnt.key(idx);
        index_t cnt = m_large_cnt.val(idx);
        large_beg[idx] = std::make_pair(key, cnt);
        sum += cnt;
        len += cnt * key;
        if (cnt_of_min_cnt_key > cnt) {
            cnt_of_min_cnt_key = cnt;
            m_min_cnt_key = key;
        }
        if (cnt_of_max_cnt_key < cnt) {
            cnt_of_max_cnt_key = cnt;
            m_max_cnt_key = key;
        }
        if (m_min_key_len > key) {
          m_min_key_len = key;
        }
        if (m_max_key_len < key) {
          m_max_key_len = key;
        }
    }
    m_distinct_key_cnt = distinct_cnt + large_num;
    m_cnt_sum = sum;
    m_total_key_len = len;
    m_cnt_of_min_cnt_key = cnt_of_min_cnt_key;
    m_cnt_of_max_cnt_key = cnt_of_max_cnt_key;
    std::sort(large_beg, large_beg + large_num);
    m_large_cnt_compact.risk_set_size(large_num);
}

template class TERARK_DLL_EXPORT Histogram<uint32_t>;
template class TERARK_DLL_EXPORT Histogram<uint64_t>;

} // namespace terark
