
#include "rank_select_fewzero.hpp"

/*

template <size_t M>
void rank_select_fewzero<M>::risk_mmap_from(unsigned char *base,
                                            size_t length) {
  size_t cnt = length / sizeof(T);
  m_mem.risk_set_data((T *)base, cnt);
  m_size = m_mem[0];
  // for 4B and odd size, Pad0ForAlign was made, skip back() '0'
  if (sizeof(T) % 8 && m_mem.back() == 0)
    cnt--;
  m_pospool.risk_set_data((T *)base + 1, cnt - 1); // skip [0]
}

// exclude pos
template <size_t M> size_t rank_select_fewzero<M>::rank0(size_t pos) const {
  assert(pos <= m_size);
  return terark::lower_bound_0(m_pospool.begin(), m_pospool.size(), pos);
}
template <size_t M> size_t rank_select_fewzero<M>::rank1(size_t pos) const {
  assert(pos <= m_size);
  return pos - rank0(pos);
}
template <size_t M> size_t rank_select_fewzero<M>::select0(size_t id) const {
  assert(id < max_rank0());
  return m_pospool[id];
}

template <size_t M> size_t rank_select_fewzero<M>::select1(size_t id) const {
  // key_ex = pos - m_pospool
  // etc.
  // pos       =  0  1  2  3  4  5  6  7  8  9 10 11 12 13 14
  // bits      =  0  1  0  1  1  0  0  0  1  1  1  0  0  1  1
  // m_pospool =  0     1        2  3  4           5  6
  // key_ex    =  0     1        3  3  3           6  6
  // we call select1 with id = 4
  //                    it's at here --------^
  // upper_bound(4) found here --------------------^
  // we got pos = 5
  // it means if we move zeros to front
  //                <<< move zeros <<<
  // bits      =  0  0  0  0  0  1  1  1  1  1  1  0  0  1  1
  // pos 5 is the 1st one -------^
  //                             >-- +id --> |
  // pos + id is our result -----------------^
  assert(id < max_rank1());
  auto begin = m_pospool.begin();
  return terark::upper_bound_ex_0(begin, m_pospool.size(), id,
                                  [&begin](const T &v) {
                                    // same as: m_pospool[idx] - idx
                                    return v - T(&v - begin);
                                  }) +
         id;
}
'

// res: as the rank0 lower_bound
template <size_t M>
bool rank_select_fewzero<M>::is1(size_t pos, size_t &rank0) const {
  assert(pos < m_size);
  rank0 = terark::lower_bound_0(m_pospool.begin(), m_pospool.size(), pos);
  bool is0 = (rank0 < m_pospool.size() && pos == m_pospool[rank0]);
  return !is0;
}
template <size_t M> bool rank_select_fewzero<M>::operator[](size_t pos) const {
  assert(pos < m_size);
  return is1(pos);
}
template <size_t M> bool rank_select_fewzero<M>::is1(size_t pos) const {
  assert(pos < m_size);
  size_t res;
  return is1(pos, res);
}
template <size_t M> bool rank_select_fewzero<M>::is0(size_t pos) const {
  assert(pos < m_size);
  return !is1(pos);
}

///@returns number of continuous one/zero bits starts at bitpos
template <size_t M>
size_t rank_select_fewzero<M>::zero_seq_len(size_t pos) const {
  assert(pos < m_size);
  size_t i;
  if (is1(pos, i))
    return 0;
  size_t cnt = 1;
  while (i < m_pospool.size() - 1) {
    if (m_pospool[i] + 1 == m_pospool[i + 1])
      i++, cnt++;
    else
      break;
  }
  return cnt;
}
template <size_t M>
size_t rank_select_fewzero<M>::zero_seq_len(size_t pos, size_t &hint) const {
  assert(pos < m_size);
  if (0 < hint && hint < m_pospool.size() - 1 && m_pospool[hint] < pos &&
      pos < m_pospool[hint + 1]) {
    return 0;
  }
  size_t i;
  if (is1(pos, i)) {
    hint = i - 1;
    return 0;
  }
  size_t cnt = 1;
  while (i < m_pospool.size() - 1) {
    if (m_pospool[i] + 1 == m_pospool[i + 1])
      i++, cnt++;
    else
      break;
  }
  return cnt;
}
template <size_t M>
size_t rank_select_fewzero<M>::zero_seq_revlen(size_t pos) const {
  assert(pos < m_size);
  RET_IF_ZERO(pos);
  pos--;
  size_t i;
  if (is1(pos, i))
    return 0;
  size_t cnt = 1;
  while (i > 1) { // m_pospool[0] is 'placeholder' -- m_size
    if (m_pospool[i - 1] + 1 == m_pospool[i])
      i--, cnt++;
    else
      break;
  }
  return cnt;
}
template <size_t M>
size_t rank_select_fewzero<M>::zero_seq_revlen(size_t pos, size_t &hint) const {
  assert(pos < m_size);
  RET_IF_ZERO(pos);
  pos--;
  if (0 < hint && hint < m_pospool.size() - 1 && m_pospool[hint] < pos &&
      pos < m_pospool[hint + 1]) {
    return 0;
  }
  size_t i;
  if (is1(pos, i)) {
    hint = i - 1;
    return 0;
  }
  size_t cnt = 1;
  while (i > 1) { // m_pospool[0] is 'placeholder' -- m_size
    if (m_pospool[i - 1] + 1 == m_pospool[i])
      i--, cnt++;
    else
      break;
  }
  return cnt;
}
template <size_t M>
size_t rank_select_fewzero<M>::one_seq_len(size_t pos) const {
  assert(pos < m_size);
  size_t i;
  if (!is1(pos, i))
    return 0;
  if (i >= m_pospool.size())
    return m_size - pos;
  else
    return m_pospool[i] - pos;
}
template <size_t M>
size_t rank_select_fewzero<M>::one_seq_revlen(size_t pos) const {
  assert(pos < m_size);
  RET_IF_ZERO(pos);
  pos--;
  size_t i;
  if (!is1(pos, i))
    return 0;
  if (i == 0)
    return pos + 1; // add back '1'
  else
    return pos - m_pospool[i - 1];
}

template <size_t M>
void rank_select_fewone<M>::risk_mmap_from(unsigned char *base, size_t length) {
  size_t cnt = length / sizeof(T);
  m_mem.risk_set_data((T *)base, cnt);
  m_size = m_mem[0];
  // for 4B and odd size, Pad0ForAlign was made, skip back() '0'
  if (sizeof(T) % 8 && m_mem.back() == 0)
    cnt--;
  m_pospool.risk_set_data((T *)base + 1, cnt - 1); // skip [0]
}

template <size_t M> size_t rank_select_fewone<M>::rank0(size_t pos) const {
  assert(pos < m_size);
  RET_IF_ZERO(pos);
  return pos - rank1(pos);
}
template <size_t M> size_t rank_select_fewone<M>::rank1(size_t pos) const {
  assert(pos < m_size);
  // rank1 exclude pos + rank1 start from '1' => lower_bound works
  return terark::lower_bound_n<const valvec<M> &>(m_pospool, 0,
                                                  m_pospool.size(), pos);
}
template <size_t M> size_t rank_select_fewone<M>::select0(size_t id) const {
  assert(id < max_rank1());
  auto begin = m_pospool.begin();
  return terark::upper_bound_ex_0(
             begin, m_pospool.size(), id,
             [&begin](const T &v) { return v - T(&v - begin); }) +
         id;
}
template <size_t M> size_t rank_select_fewone<M>::select1(size_t id) const {
  assert(id < max_rank1());
  return m_pospool[id];
}

// res: as the rank0 lower_bound
template <size_t M>
bool rank_select_fewone<M>::is0(size_t pos, size_t &rank1) const {
  assert(pos < m_size);
  rank1 = terark::lower_bound_n<const terark::valvec<M> &>(
      m_pospool, 0, m_pospool.size(), pos);
  bool is1 = (rank1 < m_pospool.size() && pos == m_pospool[rank1]);
  return !is1;
}
template <size_t M> bool rank_select_fewone<M>::operator[](size_t pos) const {
  assert(pos < m_size);
  return is1(pos);
}
template <size_t M> bool rank_select_fewone<M>::is0(size_t pos) const {
  assert(pos < m_size);
  size_t res;
  return is0(pos, res);
}
template <size_t M> bool rank_select_fewone<M>::is1(size_t pos) const {
  assert(pos < m_size);
  return !is0(pos);
}

///@returns number of continuous one/zero bits starts at bitpos
template <size_t M>
size_t rank_select_fewone<M>::zero_seq_len(size_t pos) const {
  assert(pos < m_size);
  size_t i;
  if (!is0(pos, i))
    return 0;
  if (i >= m_pospool.size())
    return m_size - pos;
  else
    return m_pospool[i] - pos;
}
template <size_t M>
size_t rank_select_fewone<M>::zero_seq_len(size_t pos, size_t &hint) const {
  assert(pos < m_size);
  if (0 < hint && hint < m_pospool.size() - 1 && m_pospool[hint] < pos &&
      pos < m_pospool[hint + 1]) {
    hint++; // to next '1' where following Next() will start from
    return m_pospool[hint] - pos;
  }
  size_t i;
  if (!is0(pos, i)) {
    hint = i - 1;
    return 0;
  }
  if (i >= m_pospool.size())
    return m_size - pos;
  else
    return m_pospool[i] - pos;
}
template <size_t M>
size_t rank_select_fewone<M>::zero_seq_revlen(size_t pos) const {
  assert(pos < m_size);
  RET_IF_ZERO(pos);
  pos--;
  size_t i;
  if (!is0(pos, i))
    return 0;
  if (i == 0)
    return pos + 1;
  else
    return pos - m_pospool[i - 1];
}
template <size_t M>
size_t rank_select_fewone<M>::zero_seq_revlen(size_t pos, size_t &hint) const {
  assert(pos < m_size);
  RET_IF_ZERO(pos);
  pos--;
  if (0 < hint && hint < m_pospool.size() - 1 && m_pospool[hint] < pos &&
      pos < m_pospool[hint + 1]) {
    size_t cur = hint--; // to last '1' where following Prev() will start from
    return pos - m_pospool[cur];
  }
  size_t i;
  if (!is0(pos, i)) {
    hint = i - 1;
    return 0;
  }
  if (i == 0)
    return pos + 1;
  else
    return pos - m_pospool[i - 1];
}

template <size_t P, size_t W>
size_t rank_select_few<>::one_seq_len(size_t pos) const {
  assert(pos < m_size);
  size_t i;
  if (is0(pos, i))
    return 0;
  size_t cnt = 1;
  while (i < m_pospool.size() - 1) {
    if (m_pospool[i] + 1 == m_pospool[i + 1])
      i++, cnt++;
    else
      break;
  }
  return cnt;
}
template <size_t M>
size_t rank_select_fewone<M>::one_seq_revlen(size_t pos) const {
  assert(pos < m_size);
  RET_IF_ZERO(pos);
  pos--;
  size_t i;
  if (is0(pos, i))
    return 0;
  size_t cnt = 1;
  while (i > 1) { // m_pospool[0] is placeholder
    if (m_pospool[i - 1] + 1 == m_pospool[i])
      i--, cnt++;
    else
      break;
  }
  return cnt;
}
*/

namespace terark {

#define RET_IF_ZERO(v)                                                         \
  do {                                                                         \
    if ((v) == 0)                                                              \
      return 0;                                                                \
  } while (0)

template <size_t P, size_t W>
size_t rank_select_few<P, W>::lower_bound(size_t val) const {
  const uint8_t *base = m_mempool.data();
  const uint8_t *p = base + ((*m_layer == 1) ? 0 : m_offset[*m_layer - 2]);
  for (auto i = *m_layer - 2; i >= 0; --i) {
      for (const uint8_t *e = base + m_offset[i + 1]; p < e; p += W) {
          if (val_at_ptr(p) >= val)
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
  if (hint < (P ? *m_num1 : *m_num0))
    if (val_a_logi(hint) == val)
      return hint;
  hint = lower_bound(val);
  return hint;
}

template <size_t P, size_t W>
size_t rank_select_few<P, W>::select_complement(size_t id) const {
  assert(id <= (P ? *m_num0 : *m_num1));
  const uint8_t *base = m_mempool.data();
  if(id < val_at_ptr(base)){
      return id;
  }
  if(id + (P ? *m_num1 : *m_num0) >= val_at_ptr(base + m_offset[0] - W)){
      return id + (P ? *m_num1 : *m_num0);
  }
  const uint8_t *p = base + ((*m_layer == 1) ? 0 : m_offset[*m_layer - 2]);
  const uint8_t *e;
  size_t pos = 0;
  for( auto i = *m_layer - 2; i >= 0; --i){
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
  size_t n = P ? *m_num1 : *m_num0;
  if (hint == 0) {
    if (id < val_a_logi(0))
      return id;
    else if (id < val_a_logi(1))
      return id - 1;
  } else if (hint == n - 1) {
    if (id + n > val_a_logi(n - 1))
      return id + n - 1;
    else if (id + n - 1 > val_a_logi(n - 2))
      return id + n - 2;
  } else if (hint < n) {
    size_t pre = val_a_logi(hint - 1);
    size_t now = val_a_logi(hint);
    if ((pre <= id + hint) && (id + hint < now))
      return id + hint - 1;
    size_t nxt = val_a_logi(hint + 1);
    if ((now <= id + hint) && (id + hint < nxt))
      return id + hint;
  }
  return select_complement(id);
}
template <size_t P, size_t W>
bool rank_select_few<P, W>::operator[](size_t pos) const {
  if (P) {
    return lower_bound(pos) == pos;
  } else {
    return lower_bound(pos) != pos;
  }
}

template <size_t P, size_t W>
size_t rank_select_few<P, W>::rank0(size_t pos) const {
  if (P) {
    return pos - rank1(pos);
  } else {
    return lower_bound(pos);
  }
}

template <size_t P, size_t W>
size_t rank_select_few<P, W>::rank0(size_t pos, size_t &hint) {
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
    return pos - rank0(pos);
  }
}

template <size_t P, size_t W>
size_t rank_select_few<P, W>::rank1(size_t pos, size_t &hint) {
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
size_t rank_select_few<P, W>::select0(size_t id, size_t &hint) {
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
size_t rank_select_few<P, W>::select1(size_t id, size_t &hint) {
  if (P) {
    return val_a_logi(id);
  } else {
    return select_complement(id, hint);
  }
}

template <size_t P, size_t W>
size_t rank_select_few<P, W>::zero_seq_len(size_t pos) const {
  if (P) {
    return val_a_logi(lower_bound(pos)+1) - pos;
  } else {
    size_t a;
    if (is1(pos, a))
      return 0;
    size_t cnt = 1, prev, now = val_a_logi(a);
    while (++a < m_offset[0]) {
      prev = now;
      now = val_a_logi(a);
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
  if (P) {
    return val_a_logi(lower_bound(pos, hint)+1) - pos;
  } else {
    if (is1(pos, hint))
      return 0;
    size_t cnt = 1, prev, now = val_a_logi(hint);
    while (++hint < m_offset[0]) {
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
size_t rank_select_few<P, W>::zero_seq_revlen(size_t pos) const {
  if (!pos)
    return 0;
  if (P) {
    return pos - val_a_logi(lower_bound(pos) - 1);
  } else {
    size_t a;
    if (is1(pos - 1, a))
      return 0;
    size_t cnt = 1, last, now = val_a_logi(a);
    while (0 <= --a) {
      last = now;
      now = val_a_logi(a);
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
    return pos - val_a_logi(lower_bound(pos, hint) - 1);
  } else {
    if (is1(pos - 1, hint))
      return 0;
    size_t cnt = 1, last, now = val_a_logi(hint);
    while (0 <= --hint) {
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
size_t rank_select_few<P, W>::one_seq_len(size_t pos) const {
  if (P) {
    size_t a = 0xFFFFFFFF;
    if (is0(pos, a))
      return 0;
    size_t cnt = 1, prev, now = val_a_logi(a);
    while (++a > m_offset[0]) {
      prev = now;
      now = val_a_logi(a);
      if (prev + 1 == now)
        cnt++;
      else
        break;
    }
    return cnt;
  } else {
    return val_a_logi(lower_bound(pos)+1) - pos;
  }
}

template <size_t P, size_t W>
size_t rank_select_few<P, W>::one_seq_len(size_t pos, size_t &hint) const {
  if (P) {
    if (is0(pos, hint))
      return 0;
    size_t cnt = 1, prev, now = val_a_logi(hint);
    while (++hint > m_offset[0]) {
      prev = now;
      now = val_a_logi(hint);
      if (prev + 1 == now)
        cnt++;
      else
        break;
    }
    return cnt;
  } else {
    return val_a_logi(lower_bound(pos, hint)+1) - pos;
  }
}

template <size_t P, size_t W>
size_t rank_select_few<P, W>::one_seq_revlen(size_t pos) const {
  if (!pos)
    return 0;
  if (P) {
    size_t a = 0xFFFFFFFF;
    if (is0(pos - 1, a))
      return 0;
    size_t cnt = 1, last, now = val_a_logi(a);
    while (0 <= --a) {
      last = now;
      now = val_a_logi(a);
      if (last == now + 1)
        cnt++;
      else
        break;
    }
    return cnt;
  } else {
    return pos - val_a_logi(lower_bound(pos) - 1);
  }
}

template <size_t P, size_t W>
size_t rank_select_few<P, W>::one_seq_revlen(size_t pos, size_t &hint) const {
  if (!pos)
    return 0;
  if (P) {
    if (is0(pos - 1, hint))
      return 0;
    size_t cnt = 1, last, now = val_a_logi(hint);
    while (0 <= --hint) {
      last = now;
      now = val_a_logi(hint);
      if (last == now + 1)
        cnt++;
      else
        break;
    }
    return cnt;
  } else {
    return pos - val_a_logi(lower_bound(pos, hint) - 1);
  }
}

template <size_t P, size_t W>
rank_select_few_builder<P, W>::rank_select_few_builder(size_t num0, size_t num1,
                                                       bool rev) {
  size_t sz = 0;
  size_t cache[8];
  byte layer = 0;
  size_t n = (P ? num1 : num0);
  while(n){
      sz += n * W;
      cache[layer++] = sz;
      n >>= 8;
  }
  if (!P)
    m_last = rev ? num0 + num1 - 1 : 0;
  assert(layer <= 8);
  m_mempool.resize(sz + 1 + (2 + layer) * 8);
  m_num0 = reinterpret_cast<uint64_t *>(&m_mempool[sz]);
  *m_num0 = num0;
  m_num1 = reinterpret_cast<uint64_t *>(&m_mempool[sz + 8]);
  *m_num1 = num1;
  m_offset = reinterpret_cast<uint64_t *>(&m_mempool[sz + 8 * 2]);
  memcpy(m_offset, cache, layer * 8);
  m_layer = reinterpret_cast<uint8_t *>(&m_mempool[sz + 8 * (layer + 2)]);
  *m_layer = layer;
  m_it = (m_rev = rev) ? m_mempool.data() + cache[0] - W : m_mempool.data();
}

template <size_t P, size_t W>
void rank_select_few_builder<P, W>::insert(size_t pos) {
  assert(pos <= ~(0xFFFFFFFFULL << (W * 8)));
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
        *(reinterpret_cast<uint64_t *>(m_it)) |= --m_last;
        m_it -= W;
      }
    } else {
      while (m_it < m_mempool.data() + m_offset[0]) {
        *(reinterpret_cast<uint64_t *>(m_it)) |= ++m_last;
        m_it += W;
      }
    }
  }
  uint8_t *base = m_mempool.data();
  uint8_t *src = base;
  for (auto i = 0; i < *m_layer - 1; ++i) {
    for (uint8_t *dst = base + m_offset[i]; dst < base + m_offset[i + 1];) {
      memcpy(dst, src, W);
      src += W * 256;
      dst += W;
    }
    src = base + m_offset[i];
  }
  std::swap(m_offset, r->m_offset);
  std::swap(m_num0, r->m_num0);
  std::swap(m_num1, r->m_num1);
  std::swap(m_layer, r->m_layer);
  std::swap(m_mempool, r->m_mempool);
  m_it = nullptr;
  m_offset = m_num0 = m_num1 = nullptr;
  m_layer = nullptr;
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