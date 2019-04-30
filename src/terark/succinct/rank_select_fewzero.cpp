
#include "rank_select_fewzero.hpp"

namespace terark {

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::lower_bound(size_t val) const {
    const uint8_t *base = m_mempool.data();
    const uint8_t *p = base + ((m_layer == 1) ? 0 : m_offset[m_layer - 2]);
    for (int i = m_layer - 2; i >= 0; --i) {
      for (const uint8_t *e = base + m_offset[i + 1]; p < e; p += W) {
        if (val_at_ptr(p) > val)
          break;
      }
      if(p > base + m_offset[i]) p -= W;
      p = base + (i ? m_offset[i - 1] : 0) + (p - base - m_offset[i]) * 256;
    }
    for(const uint8_t *e = base + m_offset[0]; p < e; p += W){
      if (val_at_ptr(p) >= val)
        break;
    }
    return (p - base) / W;
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::lower_bound(size_t val, size_t &hint) const {
    size_t n = P ? m_num1 : m_num0;
    if (hint < n) {
      if ((hint > 0) &&
          (val_a_logi(hint - 1) < val) && (val_a_logi(hint) >= val))
        return hint;
      if ((hint + 1 < n) &&
          (val_a_logi(hint) < val) && (val_a_logi(hint + 1) >= val))
        return ++hint;
      if ((hint > 1) &&
          (val_a_logi(hint - 2) < val) && (val_a_logi(hint - 1) >= val))
        return --hint;
      if ((hint == 0) &&
          (val_a_logi(0) >= val))
        return 0;
      if ((hint + 1 == n) &&
          (val_a_logi(hint) < val))
        return n;
    }
    return hint = lower_bound(val);
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::select_complement(size_t id) const {
    assert(id < (P ? m_num0 : m_num1));
    const uint8_t *base = m_mempool.data();
    if(id < val_at_ptr(base)){
      return id;
    }
    if(id + (P ? m_num1 : m_num0) > val_at_ptr(base + m_offset[0] - W)){
      return id + (P ? m_num1 : m_num0);
    }
    const uint8_t *p = base + ((m_layer == 1) ? 0 : m_offset[m_layer - 2]);
    const uint8_t *e;
    size_t pos = 0;
    for( int i = m_layer - 2; i >= 0; --i){
      for(e = base + m_offset[i + 1]; p < e ;){
        if(id + pos < val_at_ptr(p)) break;
        p += W;
        pos += 1ULL << (8 * i + 8);
      }
      if(pos){
        p -= W;
        pos -= 1ULL << (8 * i + 8);
      }
      p = base + (i ? m_offset[i - 1] : 0) + (p - (base + m_offset[i])) * 256;
    }
    for(e = base + m_offset[0]; p < e ;){
      if(id + pos < val_at_ptr(p)) break;
      p += W;
      pos ++;
    }
    return id + pos;
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::select_complement(size_t id, size_t &hint) const {
    size_t n = P ? m_num1 : m_num0;
    if (hint < n) {
      size_t val = id + hint;
      if ((hint > 0) &&
          (val_a_logi(hint - 1) <= val - 1) && (val_a_logi(hint) > val))
        return id + hint;
      if ((hint + 1 < n) &&
          (val_a_logi(hint) <= val) && (val_a_logi(hint + 1) > val + 1))
        return id + ++hint;
      if ((hint > 1) &&
          (val_a_logi(hint - 2) <= val - 2) && (val_a_logi(hint - 1) > val - 1))
        return id + --hint;
      if ((hint + 1 == n) &&
          (val_a_logi(hint) <= val))
        return id + n;
      if ((hint == 0) &&
          (val_a_logi(0) > val))
        return id;
    }
    hint = select_complement(id);
    return hint;
  }

  template <size_t P, size_t W>
  bool rank_select_few<P, W>::operator[](size_t pos) const {
    if (P) {
      return val_a_logi(lower_bound(pos)) == pos;
    } else {
      return val_a_logi(lower_bound(pos)) != pos;
    }
  }

  template <size_t P, size_t W>
  bool rank_select_few<P, W>::at_with_hint(size_t pos, size_t &hint) const {
    if (P) {
      return val_a_logi(lower_bound(pos, hint)) == pos;
    } else {
      return val_a_logi(lower_bound(pos, hint)) != pos;
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::rank0(size_t pos) const {
    if (P) {
      return pos - lower_bound(pos);
    } else {
      return lower_bound(pos);
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::rank0(size_t pos, size_t &hint) const {
    if (P) {
      return pos - rank1(pos, hint);
    } else {
      return lower_bound(pos, hint);
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::rank1(size_t pos) const {
    if (P) {
      return lower_bound(pos);
    } else {
      return pos - lower_bound(pos);
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::rank1(size_t pos, size_t &hint) const {
    if (P) {
      return lower_bound(pos, hint);
    } else {
      return pos - rank0(pos, hint);
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::select0(size_t id) const {
    if (P) {
      return select_complement(id);
    } else {
      return val_a_logi(id);
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::select0(size_t id, size_t &hint) const {
    if (P) {
      return select_complement(id, hint);
    } else {
      return val_a_logi(id);
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::select1(size_t id) const {
    if (P) {
      return val_a_logi(id);
    } else {
      return select_complement(id);
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::select1(size_t id, size_t &hint) const {
    if (P) {
      return val_a_logi(id);
    } else {
      return select_complement(id, hint);
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::zero_seq_len(size_t pos) const {
    size_t idx = lower_bound(pos);
    if (P) {
      if(idx == m_num1) return m_num0 + m_num1 - pos;
      return val_a_logi(idx) - pos;
    } else {
      if (val_a_logi(idx) != pos) return 0;
      size_t cnt, prev, now;
      cnt = 1;
      now = val_a_logi(idx);
      while (++idx < m_num1 + m_num0) {
        prev = now;
        now = val_a_logi(idx);
        if (prev + 1 == now)
          cnt++;
        else
          break;
      }
      return cnt;
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::zero_seq_len(size_t pos, size_t &hint) const {
    lower_bound(pos, hint);
    if (P) {
      if(hint == m_num1) return m_num0 + m_num1 - pos;
      return val_a_logi(hint) - pos;
    } else {
      if (val_a_logi(hint) != pos) return 0;
      size_t cnt, prev, now;
      cnt = 1;
      now = val_a_logi(hint);
      while (++hint < m_num1 + m_num0) {
        prev = now;
        now = val_a_logi(hint);
        if (prev + 1 == now)
          cnt++;
        else
          break;
      }
      return cnt;
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::one_seq_len(size_t pos) const {
    size_t idx = lower_bound(pos);
    if (P) {
      if (val_a_logi(idx) != pos) return 0;
      size_t cnt, prev, now;
      cnt = 1;
      now = val_a_logi(idx);
      while (++idx < m_num1 + m_num0) {
        prev = now;
        now = val_a_logi(idx);
        if (prev + 1 == now)
          cnt++;
        else
          break;
      }
      return cnt;
    } else {
      if(idx == m_num0) return m_num0 + m_num1 - pos;
      return val_a_logi(idx) - pos;
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::one_seq_len(size_t pos, size_t &hint) const {
    lower_bound(pos, hint);
    if (P) {
      if (val_a_logi(hint) != pos) return 0;
      size_t cnt, prev, now;
      cnt = 1;
      now = val_a_logi(hint);
      while (++hint < m_num1 + m_num0) {
        prev = now;
        now = val_a_logi(hint);
        if (prev + 1 == now)
          cnt++;
        else
          break;
      }
      return cnt;
    } else {
      if(hint == m_num0) return m_num0 + m_num1 - pos;
      return val_a_logi(hint) - pos;
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::zero_seq_revlen(size_t pos) const {
    if (!pos)
      return 0;
    if (P) {
      size_t idx = lower_bound(pos);
      if(idx) return pos - 1 - val_a_logi(idx-1);
      else return pos;
    } else {
      size_t idx = lower_bound(pos-1);
      if (val_a_logi(idx) != pos-1) return 0;
      size_t cnt, last, now;
      cnt = 1;
      now = val_a_logi(idx);
      while (--idx != 0xFFFFFFFFFFFFFFFF) {
        last = now;
        now = val_a_logi(idx);
        if (last == now + 1)
          cnt++;
        else
          break;
      }
      return cnt;
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::zero_seq_revlen(size_t pos, size_t &hint) const {
    if (!pos)
      return 0;
    if (P) {
      lower_bound(pos, hint);
      if(hint) return pos - 1 - val_a_logi(hint-1);
      else return pos;
    } else {
      lower_bound(pos-1, hint);
      if (val_a_logi(hint) != pos-1) return 0;
      size_t cnt, last, now;
      cnt = 1;
      now = val_a_logi(hint);
      while (--hint != 0xFFFFFFFFFFFFFFFF) {
        last = now;
        now = val_a_logi(hint);
        if (last == now + 1)
          cnt++;
        else
          break;
      }
      return cnt;
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::one_seq_revlen(size_t pos) const {
    if (!pos)
      return 0;
    if (P) {
      size_t idx = lower_bound(pos-1);
      if (val_a_logi(idx) != pos-1) return 0;
      size_t cnt, last, now;
      cnt = 1;
      now = val_a_logi(idx);
      while (--idx != 0xFFFFFFFFFFFFFFFF) {
        last = now;
        now = val_a_logi(idx);
        if (last == now + 1)
          cnt++;
        else
          break;
      }
      return cnt;
    } else {
      size_t idx = lower_bound(pos);
      if(idx) return pos - 1 - val_a_logi(idx-1);
      else return pos;
    }
  }

  template <size_t P, size_t W>
  size_t rank_select_few<P, W>::one_seq_revlen(size_t pos, size_t &hint) const {
    if (!pos)
      return 0;
    if (P) {
      lower_bound(pos-1, hint);
      if (val_a_logi(hint) != pos-1) return 0;
      size_t cnt, last, now;
      cnt = 1;
      now = val_a_logi(hint);
      while (--hint != 0xFFFFFFFFFFFFFFFF) {
        last = now;
        now = val_a_logi(hint);
        if (last == now + 1)
          cnt++;
        else
          break;
      }
      return cnt;
    } else {
      lower_bound(pos, hint);
      if(hint) return pos - 1 - val_a_logi(hint-1);
      else return pos;
    }
  }

  template <size_t P, size_t W>
  rank_select_few_builder<P, W>::rank_select_few_builder(size_t num0, size_t num1,
                                                         bool rev) {
    size_t sz = 0;
    size_t cache[8];
    size_t layer = 0;
    size_t n = (P ? num1 : num0);
    while(n){
      sz += n * W;
      cache[layer++] = sz;
      n >>= 8;
    }
    if (!P)
      m_last = rev ? num0 + num1 - 1 : 0;
    assert(layer <= 8);
    size_t align_size = (sz + 7) / 8 * 8;
    m_mempool.resize(align_size + 8 + (2 + layer) * 8);
    uint64_t * base = reinterpret_cast<uint64_t *>(m_mempool.data() + align_size);
    base[0] = m_num0 = num0;
    base[1] = m_num1 = num1;
    m_offset = base + 2;
    memcpy(m_offset, cache, layer * 8);
    base[2 + layer] = m_layer = layer;
    m_it = (m_rev = rev) ? m_mempool.data() + cache[0] - W : m_mempool.data();
  }

  template <size_t P, size_t W>
  void rank_select_few_builder<P, W>::insert(size_t pos) {
    assert((W == 8) || (pos <= ~(0xFFFFFFFFFFFFFFFFULL << (W * 8))));
    if (P) {
      *(reinterpret_cast<uint64_t *>(m_it)) |= pos;
      m_it = m_rev ? m_it - W : m_it + W;
    } else {
      if (m_rev) {
        assert(pos <= m_last);
        while (m_last != pos) {
          *(reinterpret_cast<uint64_t *>(m_it)) |= m_last--;
          m_it -= W;
        }
        m_last--;
      } else {
        assert(pos >= m_last);
        while (m_last != pos) {
          *(reinterpret_cast<uint64_t *>(m_it)) |= m_last++;
          m_it += W;
        }
        m_last++;
      }
    }
  }

  template <size_t P, size_t W>
  void rank_select_few_builder<P, W>::finish(rank_select_few<P, W> *r) {
    if (!P) {
      if (m_rev) {
        while (m_it >= m_mempool.data()) {
          *(reinterpret_cast<uint64_t *>(m_it)) |= m_last--;
          m_it -= W;
        }
      } else {
        while (m_it < m_mempool.data() + m_offset[0]) {
          *(reinterpret_cast<uint64_t *>(m_it)) |= m_last++;
          m_it += W;
        }
      }
    }
    uint8_t *base = m_mempool.data();
    uint8_t *src = base;
    for (int i = 0; i < m_layer - 1; ++i) {
      for (uint8_t *dst = base + m_offset[i]; dst < base + m_offset[i + 1];) {
        memcpy(dst, src, W);
        src += W * 256;
        dst += W;
      }
      src = base + m_offset[i];
    }
    std::swap(m_offset, r->m_offset);
    r->m_layer = m_layer;
    r->m_num0 = m_num0;
    r->m_num1 = m_num1;
    std::swap(m_mempool, r->m_mempool);
    m_it = nullptr;
    m_mempool.clear();
  }

  template class TERARK_DLL_EXPORT rank_select_few<0, 1>;
  template class TERARK_DLL_EXPORT rank_select_few<0, 2>;
  template class TERARK_DLL_EXPORT rank_select_few<0, 3>;
  template class TERARK_DLL_EXPORT rank_select_few<0, 4>;
  template class TERARK_DLL_EXPORT rank_select_few<0, 5>;
  template class TERARK_DLL_EXPORT rank_select_few<0, 6>;
  template class TERARK_DLL_EXPORT rank_select_few<0, 7>;
  template class TERARK_DLL_EXPORT rank_select_few<0, 8>;
  template class TERARK_DLL_EXPORT rank_select_few<1, 1>;
  template class TERARK_DLL_EXPORT rank_select_few<1, 2>;
  template class TERARK_DLL_EXPORT rank_select_few<1, 3>;
  template class TERARK_DLL_EXPORT rank_select_few<1, 4>;
  template class TERARK_DLL_EXPORT rank_select_few<1, 5>;
  template class TERARK_DLL_EXPORT rank_select_few<1, 6>;
  template class TERARK_DLL_EXPORT rank_select_few<1, 7>;
  template class TERARK_DLL_EXPORT rank_select_few<1, 8>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<0, 1>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<0, 2>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<0, 3>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<0, 4>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<0, 5>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<0, 6>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<0, 7>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<0, 8>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<1, 1>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<1, 2>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<1, 3>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<1, 4>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<1, 5>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<1, 6>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<1, 7>;
  template class TERARK_DLL_EXPORT rank_select_few_builder<1, 8>;

} // namespace terark
