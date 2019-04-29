#pragma once

#include <boost/intrusive_ptr.hpp>
#include <memory>
#include <terark/fstring.hpp>
#include <terark/int_vector.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/util/refcount.hpp>
#include <terark/valvec.hpp>

using terark::febitvec;
using terark::valvec;

// make sure at least one '1' and at least one '0'
/*
template <size_t M> class rank_select_fewzero {
public:
  typedef boost::mpl::false_ is_mixed;

  rank_select_fewzero() : m_size(0) {}

  ~rank_select_fewzero() { clear(); }
  void clear() {
    m_size = 0;
    m_pospool.risk_release_ownership();
    m_mem.clear();
  }
  void risk_release_ownership() {
    m_pospool.risk_release_ownership();
    m_mem.risk_release_ownership();
  }

  void risk_mmap_from(unsigned char *base, size_t length);
  size_t mem_size() const { return terark::align_up(m_mem.used_mem_size(), 8); }

  void build_cache(bool, bool) { assert(0); }
  void swap(rank_select_fewzero &another) {
    std::swap(m_size, another.m_size);
    std::swap(m_mem, another.m_mem);
    std::swap(m_pospool, another.m_pospool);
  }
  void set0(size_t) { assert(0); }
  void set1(size_t) { assert(0); }

  size_t rank0(size_t pos) const;
  size_t rank1(size_t pos) const;
  size_t select0(size_t id) const;
  size_t select1(size_t id) const;

private:
  // res: as the rank0 lower_bound
  bool is1(size_t pos, size_t &rank0) const;

public:
  size_t max_rank0() const { return m_pospool.size(); }
  size_t max_rank1() const { return m_size - m_pospool.size(); }
  size_t size() const { return m_size; }
  size_t isall0() const { return false; }
  size_t isall1() const { return false; }

  const void *data() const { return m_mem.data(); }
  bool operator[](size_t pos) const;

  bool is1(size_t pos) const;
  bool is0(size_t pos) const;

  const uint32_t *get_rank_cache() const { return NULL; }
  const uint32_t *get_sel0_cache() const { return NULL; }
  const uint32_t *get_sel1_cache() const { return NULL; }

  ///@returns number of continuous one/zero bits starts at bitpos
  size_t zero_seq_len(size_t pos) const;
  // Next() accelerate version
  size_t zero_seq_len(size_t pos, size_t &hint) const;
  size_t zero_seq_revlen(size_t pos) const;
  // Prev() accelerate version
  size_t zero_seq_revlen(size_t pos, size_t &hint) const;

  size_t one_seq_len(size_t pos) const;
  size_t one_seq_revlen(size_t pos) const;

private:
  uint64_t *m_layers;
  uint8_t *m_raw;
};

// make sure at least one '1' and at least one '0'
template <size_t M> class rank_select_fewone {
public:
  typedef boost::mpl::false_ is_mixed;

  rank_select_fewone() : m_size(0) {}
  explicit rank_select_fewone(size_t sz) : m_size(sz) {}

  ~rank_select_fewone() { clear(); }
  void clear() {
    m_size = 0;
    m_pospool.risk_release_ownership();
    m_mem.clear();
  }
  void risk_release_ownership() {
    m_pospool.risk_release_ownership();
    m_mem.risk_release_ownership();
  }

  void risk_mmap_from(unsigned char *base, size_t length);
  size_t mem_size() const {
    return terark::align_up(m_pospool.used_mem_size(), 8);
  }
  template <class RankSelect> void build_from(RankSelect &rs) {
    if (rs.isall0() || rs.isall1()) {
      THROW_STD(invalid_argument, "there should be cnt(0) > 0 && cnt(1) > 0");
    }
    // for fewzero, 0.1 is actually reasonable
    m_mem.reserve(std::max<size_t>(m_size / 10, 10));
    m_mem.push_back(m_size); // [0] for m_size
    size_t idx = 1;
    for (size_t pos = 0; pos < m_size; pos++) {
      if (rs.is1(pos)) {
        m_mem.push_back(pos);
        idx++;
      }
    }
    m_mem.push_back(0); // append extra '0' for align consideration
    m_mem.resize(idx);  // resize to the actual size, extra '0' will be kept
                        // behind for align
    m_pospool.risk_set_data(m_mem.data() + 1, idx - 1);
  }
  void build_cache(bool, bool) { assert(0); }
  void swap(rank_select_fewone &another) {
    std::swap(m_size, another.m_size);
    std::swap(m_mem, another.m_mem);
    std::swap(m_pospool, another.m_pospool);
  }
  void set0(size_t) { assert(0); }
  void set1(size_t) { assert(0); }

  // exclude pos
  size_t rank0(size_t pos) const;
  size_t rank1(size_t pos) const;
  size_t select0(size_t id) const;
  size_t select1(size_t id) const;

private:
  // res: as the rank0 lower_bound
  bool is0(size_t pos, size_t &res) const;

public:
  size_t max_rank0() const { return m_size - m_pospool.size(); }
  size_t max_rank1() const { return m_pospool.size(); }
  size_t size() const { return m_size; }
  size_t isall0() const { return false; }
  size_t isall1() const { return false; }

  // const void *data() const { return m_mem.data(); }
  bool operator[](size_t pos) const;
  bool is0(size_t pos) const;
  bool is1(size_t pos) const;

  // returns number of continuous one/zero bits starts at bitpos
  size_t zero_seq_len(size_t pos) const;
  // Next() accelerate version
  size_t zero_seq_len(size_t pos, size_t &hint) const;
  size_t zero_seq_revlen(size_t pos) const;
  // Prev() accelerate version
  size_t zero_seq_revlen(size_t pos, size_t &hint) const;

  size_t one_seq_len(size_t pos) const;
  size_t one_seq_revlen(size_t pos) const;

private:
  uint64_t *m_layers;
  uint8_t *m_raw;
};

template <size_t M> class rank_select_fews_builder {
public:
  rank_select_fews_builder();
  rank_select_fews_builder(size_t num0, size_t num1, size_t width,
                           bool lessZero, bool rev);
  ~rank_select_fews_builder();
  rank_select_fewzero<M> *finish0();
  rank_select_fewone<M> *finish1();
  void insert(size_t pos);

private:
  size_t d_prev_pos;
  bool m_rev_insert;
  uint8_t *m_now;
  uint64_t *m_in;
  uint64_t *m_layers;
  uint8_t *m_raw;
};
*/

namespace terark {

template <size_t P, size_t W> class rank_select_few_builder;

template <size_t P, size_t W> class rank_select_few {
  friend class rank_select_few_builder<P, W>;

private:
  size_t lower_bound(size_t val) const;
  size_t lower_bound(size_t val, size_t &hint) const;

  size_t select_complement(size_t id) const;
  size_t select_complement(size_t id, size_t &hint) const;

    inline size_t val_a_logi(size_t pos) const {
        return (*reinterpret_cast<const size_t *>(m_mempool.data() + pos * W)) &
               (W == 8 ? 0xFFFFFFFFULL : (1ULL << ((W * 8) & 63)) - 1);
    }

    inline size_t val_at_ptr(const uint8_t* ptr) const {
        return (*reinterpret_cast<const size_t *>(ptr)) &
               (W == 8 ? 0xFFFFFFFFULL : (1ULL << ((W * 8) & 63)) - 1);
    }

public:
  rank_select_few() {}
  //~rank_select_few();

  bool operator[](size_t pos) const;
  bool is0(size_t pos) const { return !operator[](pos); }
  bool is1(size_t pos) const { return operator[](pos); }
  bool is0(size_t pos, size_t &hint) const {
    return is0(pos);
  }
  bool is1(size_t pos, size_t &hint) const {
    return is1(pos);
  }

  size_t rank0(size_t pos) const;
  size_t rank0(size_t pos, size_t &hint) const;
  size_t rank1(size_t pos) const;
  size_t rank1(size_t pos, size_t &hint) const;
  size_t select0(size_t id) const;
  size_t select0(size_t id, size_t &hint) const;
  size_t select1(size_t id) const;
  size_t select1(size_t id, size_t &hint) const;
  size_t zero_seq_len(size_t pos) const;
  size_t zero_seq_len(size_t pos, size_t &hint) const;
  size_t zero_seq_revlen(size_t pos) const;
  size_t zero_seq_revlen(size_t pos, size_t &hint) const;
  size_t one_seq_len(size_t pos) const;
  size_t one_seq_len(size_t pos, size_t &hint) const;
  size_t one_seq_revlen(size_t pos) const;
  size_t one_seq_revlen(size_t pos, size_t &hint) const;

  size_t max_rank0() const { return *m_num0; }
  size_t max_rank1() const { return *m_num1; }
  size_t size() const { return m_mempool.size(); }
  const byte_t *data() const { return m_mempool.data(); }
  size_t mem_size() const { return m_mempool.full_mem_size(); }

  void swap(rank_select_few &r) {
    std::swap(m_num0, r.m_num0);
    std::swap(m_num1, r.m_num1);
    std::swap(m_offset, r.m_offset);
    std::swap(m_layer, r.m_layer);
    m_mempool.swap(r.m_mempool);
  }

  void risk_mmap_from(unsigned char *src, size_t size) {
    m_mempool.risk_set_data(src);
    m_mempool.risk_set_size(size);
    m_layer = &m_mempool[size - 1];
    assert(*m_layer < 8);
    int offset_size = ((*m_layer == 1) ? 1 : (*m_layer - 2) * 8);
    m_offset =
        reinterpret_cast<uint64_t *>(&m_mempool[size - 1 - offset_size]);
    m_num1 =
        reinterpret_cast<uint64_t *>(&m_mempool[size - 1 - offset_size - 8]);
    m_num0 =
        reinterpret_cast<uint64_t *>(&m_mempool[size - 1 - offset_size - 16]);
  }
  void risk_release_ownership() { m_mempool.risk_release_ownership(); }

private:
  uint64_t *m_num0, *m_num1, *m_offset;
  uint8_t *m_layer;
  valvec<uint8_t> m_mempool;
};

template <size_t P, size_t W> class rank_select_few_builder {
  friend class rank_select_few<P, W>;

public:
  rank_select_few_builder(size_t num0, size_t num1, bool rev);
  ~rank_select_few_builder() {}
  void insert(size_t pos);
  void finish(rank_select_few<P, W> *);

private:
  bool m_rev;
  uint64_t m_last;
  uint8_t *m_it;
  uint64_t *m_num0, *m_num1, *m_offset;
  uint8_t *m_layer;
  valvec<uint8_t> m_mempool;
};

template <size_t W> using rank_select_fewzero = rank_select_few<0, W>;
template <size_t W> using rank_select_fewone = rank_select_few<1, W>;

template<class T>
class rank_select_hint_wrapper {
  const T& rs_;
public:
  rank_select_hint_wrapper(const T& rs, size_t* hint) : rs_(rs) {}

  bool operator[](size_t pos) const { return rs_.is1(pos); }
  bool is0(size_t pos) const { return rs_.is0(pos); }
  bool is1(size_t pos) const { return rs_.is1(pos); }
  size_t rank0(size_t pos) const { return rs_.rank0(pos); }
  size_t rank1(size_t pos) const { return rs_.rank1(pos); }
  size_t select0(size_t pos) const { return rs_.select0(pos); }
  size_t select1(size_t pos) const { return rs_.select1(pos); }
  size_t zero_seq_len(size_t pos) const { return rs_.zero_seq_len(pos); }
  size_t zero_seq_revlen(size_t pos) const { return rs_.zero_seq_revlen(pos); }
  size_t one_seq_len(size_t pos) const { return rs_.one_seq_len(pos); }
  size_t one_seq_revlen(size_t pos) const { return rs_.one_seq_revlen(pos); }

  size_t max_rank0() const { return rs_.max_rank0(); }
  size_t max_rank1() const { return rs_.max_rank1(); }
  size_t size() const { return rs_.size(); }
};

template<size_t P, size_t W>
class rank_select_hint_wrapper<rank_select_few<P, W>> {
  const rank_select_few<P, W>& rs_;
  size_t* h_;
public:
  rank_select_hint_wrapper(const rank_select_few<P, W>& rs, size_t* hint) : rs_(rs), h_(hint) {
    assert(hint != nullptr);
  }

  bool operator[](size_t pos) const { return rs_.is1(pos, *h_); }
  bool is0(size_t pos) const { return rs_.is0(pos, *h_); }
  bool is1(size_t pos) const { return rs_.is1(pos, *h_); }
  size_t rank0(size_t pos) const { return rs_.rank0(pos, *h_); }
  size_t rank1(size_t pos) const { return rs_.rank1(pos, *h_); }
  size_t select0(size_t pos) const { return rs_.select0(pos, *h_); }
  size_t select1(size_t pos) const { return rs_.select1(pos, *h_); }
  size_t zero_seq_len(size_t pos) const { return rs_.zero_seq_len(pos, *h_); }
  size_t zero_seq_revlen(size_t pos) const { return rs_.zero_seq_revlen(pos, *h_); }
  size_t one_seq_len(size_t pos) const { return rs_.one_seq_len(pos, *h_); }
  size_t one_seq_revlen(size_t pos) const { return rs_.one_seq_revlen(pos, *h_); }

  size_t max_rank0() const { return rs_.max_rank0(); }
  size_t max_rank1() const { return rs_.max_rank1(); }
  size_t size() const { return rs_.size(); }
};

template<class T>
rank_select_hint_wrapper<T> make_rank_select_hint_wrapper(const T& rs, size_t* hint) {
  return rank_select_hint_wrapper<T>(rs, hint);
}

} // namespace terark
