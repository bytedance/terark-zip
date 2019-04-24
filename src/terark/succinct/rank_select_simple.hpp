#ifndef __terark_rank_select_simple_hpp__
#define __terark_rank_select_simple_hpp__

#include "rank_select_basic.hpp"

namespace terark {

class TERARK_DLL_EXPORT rank_select_simple
    : public RankSelectConstants<256>, public febitvec {
    uint32_t* m_rank_cache;
    size_t    m_max_rank0;
    size_t    m_max_rank1;
public:
    typedef boost::mpl::false_ is_mixed;
    typedef uint32_t index_t;
    rank_select_simple();
    rank_select_simple(size_t n, bool val = false);
    rank_select_simple(size_t n, valvec_no_init);
    rank_select_simple(size_t n, valvec_reserve);
    rank_select_simple(const rank_select_simple&);
    rank_select_simple& operator=(const rank_select_simple&);
#if defined(HSM_HAS_MOVE)
    rank_select_simple(rank_select_simple&& y) noexcept;
    rank_select_simple& operator=(rank_select_simple&& y) noexcept;
#endif
    ~rank_select_simple();
    void clear();
    void risk_release_ownership();
    void risk_mmap_from(unsigned char* base, size_t length);
    void shrink_to_fit();

    void swap(rank_select_simple&);
    void build_cache(bool speed_select0, bool speed_select1);
    size_t mem_size() const;
    size_t rank1(size_t bitpos) const;
    size_t rank0(size_t bitpos) const { return bitpos - rank1(bitpos); }
    size_t select1(size_t id) const;
    size_t select0(size_t id) const;
    size_t max_rank1() const { return m_max_rank1; }
    size_t max_rank0() const { return m_max_rank0; }
    bool isall0() const { return m_max_rank1 == 0; }
    bool isall1() const { return m_max_rank0 == 0; }

    const uint32_t* get_rank_cache() const { return m_rank_cache; }
    const uint32_t* get_sel0_cache() const { return NULL; }
    const uint32_t* get_sel1_cache() const { return NULL; }
    static inline size_t fast_rank0(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos);
    static inline size_t fast_rank1(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos);
    static inline size_t fast_select0(const bm_uint_t* bits, const uint32_t* sel0Cache, const uint32_t* rankCache, size_t id);
    static inline size_t fast_select1(const bm_uint_t* bits, const uint32_t* sel1Cache, const uint32_t* rankCache, size_t id);

    size_t excess1(size_t bp) const { return 2*rank1(bp) - bp; }
    static size_t fast_excess1(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos)
        { return 2 * fast_rank1(bits, rankCache, bitpos) - bitpos; }
};

inline size_t rank_select_simple::
fast_rank0(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos) {
    return bitpos - fast_rank1(bits, rankCache, bitpos);
}

inline size_t rank_select_simple::
fast_rank1(const bm_uint_t* bits, const uint32_t* rankCache, size_t bitpos) {
    size_t line_wordpos = (bitpos & ~(LineBits - 1)) / WordBits;
    size_t line_word_idxupp = bitpos / WordBits;
    size_t rank = rankCache[bitpos / LineBits];
    for (size_t i = line_wordpos; i < line_word_idxupp; ++i)
        rank += fast_popcount(bits[i]);
    if (bitpos % WordBits != 0)
        rank += fast_popcount_trail(bits[line_word_idxupp], bitpos % WordBits);
    return rank;
}

inline size_t rank_select_simple::
fast_select0(const bm_uint_t* bits, const uint32_t* sel0, const uint32_t* rankCache, size_t rank) {
    THROW_STD(invalid_argument, "not supported");
}

inline size_t rank_select_simple::
fast_select1(const bm_uint_t* bits, const uint32_t* sel1, const uint32_t* rankCache, size_t rank) {
    THROW_STD(invalid_argument, "not supported");
}

class TERARK_DLL_EXPORT rank_select_allzero {
public:
    typedef boost::mpl::false_ is_mixed;
    typedef uint32_t index_t;
    rank_select_allzero() : m_size(0), m_placeholder(nullptr) {}
    explicit
    rank_select_allzero(size_t sz) : m_size(sz), m_placeholder(nullptr) {}

    void clear() { m_size = 0; }
    void risk_release_ownership() {}
    void risk_mmap_from(unsigned char* base, size_t length) {
        assert(base != nullptr);
        assert(length == sizeof(*this));
        m_size = *((size_t*)base);
    }
    void shrink_to_fit() {}

    void resize(size_t newsize) { m_size = newsize; }
    void swap(rank_select_allzero& another) {
        std::swap(m_size, another.m_size);
        std::swap(m_placeholder, another.m_placeholder);
    }
    void build_cache(bool, bool) {};

    size_t mem_size() const { return sizeof(*this); }
    void set0(size_t i) { assert(i < m_size); }
    void set1(size_t i) { assert(i < m_size); }
    size_t rank0(size_t bitpos) const { assert(bitpos <= m_size); return bitpos; }
    size_t rank1(size_t bitpos) const { return 0; }
    size_t select0(size_t id) const { assert(id < m_size); return id; }
    size_t select1(size_t id) const { return size_t(-1); }
    size_t max_rank0() const { return m_size; }
    size_t max_rank1() const { return 0; }
    size_t size() const { return m_size; }
    bool isall0() const { return true; }
    bool isall1() const { return false; }

    const void* data() const { return this; }
    bool operator[](size_t n) const { assert(n < m_size); return false; }
    bool is1(size_t i) const { assert(i < m_size); return false; }
    bool is0(size_t i) const { assert(i < m_size); return true;  }

    const uint32_t* get_rank_cache() const { return NULL; }
    const uint32_t* get_sel0_cache() const { return NULL; }
    const uint32_t* get_sel1_cache() const { return NULL; }

    ///@returns number of continuous one/zero bits starts at bitpos
    size_t zero_seq_len(size_t bitpos) const {
        assert(bitpos < m_size);
        return m_size - bitpos;
    }
    size_t zero_seq_revlen(size_t bitpos) const {
        assert(bitpos < m_size);
        return bitpos;
    }
    size_t one_seq_len(size_t bitpos) const { return 0; }
    size_t one_seq_revlen(size_t bitpos) const { return 0; }

private:
    size_t m_size;
    unsigned char* m_placeholder;
};

class TERARK_DLL_EXPORT rank_select_allone {
public:
    typedef boost::mpl::false_ is_mixed;
    typedef uint32_t index_t;
    rank_select_allone() : m_size(0), m_placeholder(nullptr) {}
    explicit
    rank_select_allone(size_t sz) : m_size(sz), m_placeholder(nullptr) {}

    void clear() { m_size = 0; }
    void risk_release_ownership() {}
    void risk_mmap_from(unsigned char* base, size_t length) {
        assert(base != nullptr);
        assert(length == sizeof(*this));
        m_size = *((size_t*)base);
    }
    void shrink_to_fit() {}

    void resize(size_t newsize) { m_size = newsize; }
    void swap(rank_select_allone& another) {
        std::swap(m_size, another.m_size);
        std::swap(m_placeholder, another.m_placeholder);
    }
    void build_cache(bool, bool) {};
    size_t mem_size() const { return sizeof(*this); }
    void set0(size_t i) { assert(i < m_size); }
    void set1(size_t i) { assert(i < m_size); }
    size_t rank0(size_t bitpos) const { return 0; }
    size_t rank1(size_t bitpos) const { assert(bitpos <= m_size); return bitpos; }
    size_t select0(size_t id) const { return size_t(-1); }
    size_t select1(size_t id) const { assert(id < m_size); return id; }
    size_t max_rank0() const { return 0; }
    size_t max_rank1() const { return m_size; }
    size_t size() const { return m_size; }
    bool isall0() const { return false; }
    bool isall1() const { return true; }

    const void* data() const { return this; }
    bool operator[](size_t n) const { assert(n < m_size); return true; }
    bool is1(size_t i) const { assert(i < m_size); return true;  }
    bool is0(size_t i) const { assert(i < m_size); return false; }

    const uint32_t* get_rank_cache() const { return NULL; }
    const uint32_t* get_sel0_cache() const { return NULL; }
    const uint32_t* get_sel1_cache() const { return NULL; }

    ///@returns number of continuous one/zero bits starts at bitpos
    size_t zero_seq_len(size_t bitpos) const { return 0; }
    size_t zero_seq_revlen(size_t bitpos) const { return 0; }
    size_t one_seq_len(size_t bitpos) const {
        assert(bitpos < m_size);
        return m_size - bitpos;
    }
    size_t one_seq_revlen(size_t bitpos) const {
        assert(bitpos < m_size);
        return bitpos;
    }

private:
    size_t m_size;
    unsigned char* m_placeholder;
};


} // namespace terark

#endif // __terark_rank_select_simple_hpp__

