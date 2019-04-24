#include "rank_select_mixed_xl_256.hpp"

#define GUARD_MAX_RANK(B, rank) \
    assert(rank < m_max_rank##B);

namespace terark {

template<size_t Arity>
rank_select_mixed_xl_256<Arity>::rank_select_mixed_xl_256() {
    m_lines = NULL;
    m_capacity = 0;
    for (size_t i = 0; i < Arity; ++i) {
        m_size[i] = 0;
    }
    nullize_cache();
}

namespace {
    template<size_t index, size_t Arity, class RS>
    struct SetValues {
        static void set(RS* rs, size_t beg, size_t end, bool val) {
            rs->template get<index>().set(beg, end, val);
            SetValues<index + 1, Arity, RS>::set(rs, beg, end, val);
        }
    };
    template<size_t Arity, class RS>
    struct SetValues<Arity, Arity, RS> {
        static void set(RS* rs, size_t beg, size_t end, bool val) {}
    };
}

template<size_t Arity>
rank_select_mixed_xl_256<Arity>::rank_select_mixed_xl_256(size_t n, bool val) {
    rank_select_check_overflow(n, >, rank_select_mixed_xl_256);
    m_capacity = BitsToLines(n) * sizeof(RankCacheMixed);
    for (size_t i = 0; i < Arity; ++i) m_size[i] = n;
    m_lines = (RankCacheMixed*)malloc(m_capacity);
    if (NULL == m_lines)
        throw std::bad_alloc();
    SetValues<0, Arity, rank_select_mixed_xl_256>::set(this, 0, n, val);
    nullize_cache();
}

template<size_t Arity>
rank_select_mixed_xl_256<Arity>::rank_select_mixed_xl_256(size_t n, valvec_no_init) {
    rank_select_check_overflow(n, >, rank_select_mixed_xl_256);
    m_capacity = BitsToLines(n) * sizeof(RankCacheMixed);
    for (size_t i = 0; i < Arity; ++i) m_size[i] = n;
    m_lines = (RankCacheMixed*)malloc(m_capacity);
    if (NULL == m_lines)
        throw std::bad_alloc();
    nullize_cache();
}

template<size_t Arity>
rank_select_mixed_xl_256<Arity>::rank_select_mixed_xl_256(size_t n, valvec_reserve) {
    rank_select_check_overflow(n, >, rank_select_mixed_xl_256);
    m_capacity = BitsToLines(n) * sizeof(RankCacheMixed);
    for (size_t i = 0; i < Arity; ++i) m_size[i] = n;
    m_lines = (RankCacheMixed*)malloc(m_capacity);
    if (NULL == m_lines)
        throw std::bad_alloc();
    nullize_cache();
}

template<size_t Arity>
rank_select_mixed_xl_256<Arity>::rank_select_mixed_xl_256(const rank_select_mixed_xl_256& y)
    : rank_select_mixed_xl_256(y.m_capacity, valvec_reserve())
{
    assert(this != &y);
    assert(y.m_capacity % sizeof(bm_uint_t) == 0);
    assert(y.m_size[0] == y.m_size[1]);
    memcpy(this->m_lines, y.m_lines, y.m_capacity);
    for (size_t i = 0; i < Arity; ++i) {
        if (y.m_sel0_cache[i]) {
            m_sel0_cache[i] = (uint32_t*)m_lines + (y.m_sel0_cache[i] - (uint32_t*)y.m_lines);
        }
        if (y.m_sel1_cache[i]) {
            m_sel1_cache[i] = (uint32_t*)m_lines + (y.m_sel1_cache[i] - (uint32_t*)y.m_lines);
        }
        m_size[i] = y.m_size[i];
        m_max_rank0[i] = y.m_max_rank0[i];
        m_max_rank1[i] = y.m_max_rank1[i];
    }
}

template<size_t Arity>
rank_select_mixed_xl_256<Arity>&
rank_select_mixed_xl_256<Arity>::operator=(const rank_select_mixed_xl_256& y) {
    if (this != &y) {
        this->clear();
        new(this)rank_select_mixed_xl_256(y);
    }
    return *this;
}

#if defined(HSM_HAS_MOVE)

template<size_t Arity>
rank_select_mixed_xl_256<Arity>::rank_select_mixed_xl_256(rank_select_mixed_xl_256&& y) noexcept {
    memcpy(this, &y, sizeof(*this));
    y.risk_release_ownership();
}

template<size_t Arity>
rank_select_mixed_xl_256<Arity>&
rank_select_mixed_xl_256<Arity>::operator=(rank_select_mixed_xl_256&& y) noexcept {
    if (m_lines)
        ::free(m_lines);
    memcpy(this, &y, sizeof(*this));
    y.risk_release_ownership();
    return *this;
}

#endif

template<size_t Arity>
rank_select_mixed_xl_256<Arity>::~rank_select_mixed_xl_256() {
    if (m_lines)
        ::free(m_lines);
}

template<size_t Arity>
void rank_select_mixed_xl_256<Arity>::clear() {
    if (m_lines)
        ::free(m_lines);
    risk_release_ownership();
}

template<size_t Arity>
void rank_select_mixed_xl_256<Arity>::risk_release_ownership() {
    nullize_cache();
    m_lines = nullptr;
    m_capacity = 0;
    for (size_t i = 0; i < Arity; ++i) m_size[i] = 0;
}

template<size_t Arity>
void rank_select_mixed_xl_256<Arity>::risk_mmap_from(unsigned char* base, size_t length) {
    assert(NULL == m_lines);
    assert(length % sizeof(uint64_t) == 0);
    m_flags = ((uint64_t*)(base + length))[-1];
    for (size_t i = 0; i < Arity; ++i) {
        m_size[i] = ((uint32_t*)(base + length))[-2 - Arity + i];
    }
    size_t ceiled_bits = (std::max(m_size[0], m_size[1]) + LineBits - 1) & ~size_t(LineBits - 1);
    size_t nlines = ceiled_bits / LineBits;
    m_lines = (RankCacheMixed*)base;
    m_capacity = length;
    uint32_t* select_index = (uint32_t*)(m_lines + nlines + 1);
    uint64_t  one = 1;
    auto load_data = [&](size_t dimensions) {
        if (m_flags & (one << (1 + 3 * dimensions))) {
            m_max_rank1[dimensions] = m_lines[nlines].mixed[dimensions].base;
            m_max_rank0[dimensions] = m_size[dimensions] - m_max_rank1[dimensions];
            size_t select0_slots = (m_max_rank0[dimensions] + LineBits - 1) / LineBits;
            size_t select1_slots = (m_max_rank1[dimensions] + LineBits - 1) / LineBits;
            if (m_flags & (one << (2 + 3 * dimensions)))
                m_sel0_cache[dimensions] = select_index, select_index += select0_slots + 1;
            if (m_flags & (one << (3 + 3 * dimensions)))
                m_sel1_cache[dimensions] = select_index, select_index += select1_slots + 1;
        }
    };
    if (m_flags_debug.is_first_load_d1) {
        // old format
        assert(Arity == 2);
        load_data(1);
        load_data(0);
    }
    else {
        for (size_t i = 0; i < Arity; ++i) {
            load_data(i);
        }
    }
}

template<size_t Arity>
void rank_select_mixed_xl_256<Arity>::shrink_to_fit() {
    for (size_t i = 0; i < Arity; ++i) {
        assert(NULL == m_sel0_cache[i]);
        assert(NULL == m_sel1_cache[i]);
        assert(0 == m_max_rank0[i]);
        assert(0 == m_max_rank1[i]);
    }
    size_t size = *std::max_element(m_size, m_size + Arity);
    size_t new_bytes = ((size + LineBits - 1) & ~(LineBits - 1)) / LineBits * sizeof(RankCacheMixed);
    auto new_lines = (RankCacheMixed*)realloc(m_lines, new_bytes);
    if (NULL == new_lines)
        throw std::bad_alloc();
    m_lines = new_lines;
    m_capacity = new_bytes;
}

template<size_t Arity>
void rank_select_mixed_xl_256<Arity>::swap(rank_select_mixed_xl_256& y) {
    std::swap(m_lines     , y.m_lines     );
    std::swap(m_size      , y.m_size      );
    std::swap(m_capacity  , y.m_capacity  );
    std::swap(m_flags     , y.m_flags     );
    std::swap(m_sel0_cache, y.m_sel0_cache);
    std::swap(m_sel1_cache, y.m_sel1_cache);
    std::swap(m_max_rank0 , y.m_max_rank0 );
    std::swap(m_max_rank1 , y.m_max_rank1 );
}

template<size_t Arity>
void rank_select_mixed_xl_256<Arity>::grow() {
    assert(*std::max_element(m_size, m_size + Arity) == m_capacity / sizeof(RankCacheMixed) * LineBits);
    assert((m_flags & (1 << 1)) == 0);
    assert((m_flags & (1 << 4)) == 0);
    assert((m_flags & (1 << 7)) == 0);
    assert((m_flags & (1 <<11)) == 0);
    size_t newcapBytes = 2 * std::max(m_capacity, sizeof(RankCacheMixed));
    auto new_lines = (RankCacheMixed*)realloc(m_lines, newcapBytes);
    if (NULL == new_lines)
        throw std::bad_alloc();
    if (g_Terark_hasValgrind) {
        byte_t* q = (byte_t*)new_lines;
        memset(q + m_capacity, 0, newcapBytes - m_capacity);
    }
    m_lines = new_lines;
    m_capacity = newcapBytes;
}

template<size_t Arity>
void rank_select_mixed_xl_256<Arity>::reserve_bytes(size_t newcapBytes) {
    assert(newcapBytes % sizeof(uint32_t) == 0);
    if (newcapBytes <= m_capacity)
        return;
    auto new_lines = (RankCacheMixed*)realloc(m_lines, newcapBytes);
    if (NULL == new_lines)
        throw std::bad_alloc();
    if (g_Terark_hasValgrind) {
        byte_t* q = (byte_t*)new_lines;
        memset(q + m_capacity, 0, newcapBytes - m_capacity);
    }
    m_lines = new_lines;
    m_capacity = newcapBytes;
}

template<size_t Arity>
void rank_select_mixed_xl_256<Arity>::reserve(size_t bits_capacity) {
    assert(bits_capacity % LineBits == 0);
    reserve_bytes(bits_capacity / LineBits * sizeof(RankCacheMixed));
}

template<size_t Arity>
void rank_select_mixed_xl_256<Arity>::nullize_cache() {
    m_flags = 0;
    for (size_t i = 0; i < Arity; ++i) {
        m_sel0_cache[i] = NULL;
        m_sel1_cache[i] = NULL;
        m_max_rank0[i] = 0;
        m_max_rank1[i] = 0;
    }
}

template<size_t Arity>
template<size_t dimensions>
void rank_select_mixed_xl_256<Arity>::bits_range_set0_dx(size_t i, size_t k) {
    if (i == k) {
        return;
    }
    const static size_t UintBits = sizeof(bm_uint_t) * 8;
    size_t j = i / UintBits;
    if (j == (k - 1) / UintBits) {
        m_lines[j / LineWords].words[(j % LineWords) * Arity + dimensions] &= ~(bm_uint_t(-1) << i % UintBits)
            | (bm_uint_t(-2) << (k - 1) % UintBits);
    }
    else {
        if (i % UintBits) {
            m_lines[j / LineWords].words[(j % LineWords) * Arity + dimensions] &= ~(bm_uint_t(-1) << i % UintBits);
            ++j;
        }
        while (j < k / UintBits) {
            m_lines[j / LineWords].words[(j % LineWords) * Arity + dimensions] = 0;
            ++j;
        }
        if (k % UintBits)
            m_lines[j / LineWords].words[(j % LineWords) * Arity + dimensions] &= bm_uint_t(-1) << k % UintBits;
    }
}

template<size_t Arity>
template<size_t dimensions>
void rank_select_mixed_xl_256<Arity>::bits_range_set1_dx(size_t i, size_t k) {
    if (i == k) {
        return;
    }
    const static size_t UintBits = sizeof(bm_uint_t) * 8;
    size_t j = i / UintBits;
    if (j == (k - 1) / UintBits) {
        m_lines[j / LineWords].words[(j % LineWords) * Arity + dimensions] |= (bm_uint_t(-1) << i % UintBits)
            & ~(bm_uint_t(-2) << (k - 1) % UintBits);
    }
    else {
        if (i % UintBits) {
            m_lines[j / LineWords].words[(j % LineWords) * Arity + dimensions] |= (bm_uint_t(-1) << i % UintBits);
            ++j;
        }
        while (j < k / UintBits) {
            m_lines[j / LineWords].words[(j % LineWords) * Arity + dimensions] = bm_uint_t(-1);
            ++j;
        }
        if (k % UintBits)
            m_lines[j / LineWords].words[(j % LineWords) * Arity + dimensions] |= ~(bm_uint_t(-1) << k % UintBits);
    }
}

template<size_t Arity>
void rank_select_mixed_xl_256<Arity>::
build_cache_impl(bool speed_select0,
                 bool speed_select1,
                 size_t dimensions,
                 void (rank_select_mixed_xl_256<Arity>::*bits_range_set0)(size_t, size_t)) {
    rank_select_check_overflow(m_size[dimensions], > , rank_select_mixed_se_512);
    if (NULL == m_lines) return;
    uint64_t  one = 1;

    assert((m_flags & (one << (1 + 3 * dimensions))) == 0);
    m_flags |= (one << (1 + 3 * dimensions));
    m_flags |= (uint64_t(!!speed_select0) << (2 + 3 * dimensions));
    m_flags |= (uint64_t(!!speed_select1) << (3 + 3 * dimensions));

    size_t ceiled_bits = (*std::max_element(m_size, m_size + Arity) + LineBits - 1) & ~(LineBits - 1);
    size_t lines = ceiled_bits / LineBits;
    size_t flag_as_u32_slots = 2 + Arity;
    size_t u32_slots_other_used = 0;
    size_t u32_slots_start = 0;
    size_t u32_slots_after = 0;
    bool need_shrink_to_fit = true;
    for (size_t i = 0; i < Arity; ++i) {
        if (i == dimensions) {
            u32_slots_start = u32_slots_other_used;
            continue;
        }
        size_t used = 0;
        if (m_flags & (one << (1 + 3 * i))) {
            size_t select0_slots = (m_max_rank0[i] + LineBits - 1) / LineBits + 1;
            size_t select1_slots = (m_max_rank1[i] + LineBits - 1) / LineBits + 1;
            used += ((m_flags & (one << (2 + 3 * i))) ? select0_slots : 0);
            used += ((m_flags & (one << (3 + 3 * i))) ? select1_slots : 0);
            need_shrink_to_fit = false;
        }
        u32_slots_other_used += used;
        if (i > dimensions) {
            u32_slots_after += used;
        }
    }
    if (need_shrink_to_fit) {
        shrink_to_fit();
        reserve_bytes((lines + 1) * sizeof(RankCacheMixed));
    }
    (this->*bits_range_set0)(m_size[dimensions], ceiled_bits);

    size_t Rank1 = 0;
    for (size_t i = 0; i < lines; ++i) {
        size_t inc = 0;
        m_lines[i].mixed[dimensions].base = (uint32_t)(Rank1);
        for (size_t j = 0; j < 4; ++j) {
            m_lines[i].mixed[dimensions].rlev[j] = (uint8_t)inc;
            inc += fast_popcount(m_lines[i].bit64[j * Arity + dimensions]);
        }
        Rank1 += inc;
    }
    m_lines[lines].mixed[dimensions].base = uint32_t(Rank1);
    for (size_t j = 0; j < 4; ++j)
        m_lines[lines].mixed[dimensions].rlev[j] = 0;
    m_max_rank0[dimensions] = m_size[dimensions] - Rank1;
    m_max_rank1[dimensions] = Rank1;
    size_t select0_slots_dx = (m_max_rank0[dimensions] + LineBits - 1) / LineBits;
    size_t select1_slots_dx = (m_max_rank1[dimensions] + LineBits - 1) / LineBits;
    size_t u32_slots = (speed_select0 ? select0_slots_dx + 1 : 0)
                     + (speed_select1 ? select1_slots_dx + 1 : 0)
                     ;
    size_t new_bytes = (lines + 1) * sizeof(RankCacheMixed)
                     + u32_slots * 4
                     + u32_slots_other_used * 4
                     + flag_as_u32_slots * 4
                     ;
    reserve_bytes(align_up(new_bytes, sizeof(bm_uint_t)));
    {
        size_t tailing_size = m_capacity - new_bytes + flag_as_u32_slots * 4;
        char* start = (char*)(m_lines + lines + 1) + u32_slots_start * 4;
        char* finish = (char*)m_lines + m_capacity;
        // move other select cache
        memmove(start + u32_slots * 4, start, u32_slots_after * 4);
        // clear size & flags at tail
        memset(finish - tailing_size, 0, tailing_size);
        // clean current mem
        memset(start, 0, u32_slots * 4);
        for (size_t i = dimensions + 1; i < Arity; ++i) {
            if (m_sel0_cache[i]) m_sel0_cache[i] += u32_slots;
            if (m_sel1_cache[i]) m_sel1_cache[i] += u32_slots;
        }
    }
    uint32_t* select_index = (uint32_t*)(m_lines + lines + 1) + u32_slots_start;
    if (speed_select0) {
        uint32_t* sel0_cache = select_index;
        sel0_cache[dimensions] = 0;
        for (size_t j = 1; j < select0_slots_dx; ++j) {
            size_t k = sel0_cache[j - 1];
            while (k * LineBits - m_lines[k].mixed[dimensions].base < LineBits * j) ++k;
            sel0_cache[j] = k;
        }
        sel0_cache[select0_slots_dx] = lines;
        m_sel0_cache[dimensions] = sel0_cache;
        select_index += select0_slots_dx + 1;
    }
    if (speed_select1) {
        uint32_t* sel1_cache = select_index;
        sel1_cache[dimensions] = 0;
        for (size_t j = 1; j < select1_slots_dx; ++j) {
            size_t k = sel1_cache[j - 1];
            while (m_lines[k].mixed[dimensions].base < LineBits * j) ++k;
            sel1_cache[j] = k;
        }
        sel1_cache[select1_slots_dx] = lines;
        m_sel1_cache[dimensions] = sel1_cache;
    }
    ((uint64_t*)((char*)m_lines + m_capacity))[-1] = m_flags;
    for (size_t i = 0; i < Arity; ++i) {
        ((uint32_t*)((char*)m_lines + m_capacity))[-2 - Arity + i] = uint32_t(m_size[i]);
    }
}

template<size_t Arity>
template<size_t dimensions>
size_t rank_select_mixed_xl_256<Arity>::one_seq_len_dx(size_t bitpos) const {
    assert(bitpos < m_size[dimensions]);
    size_t j = bitpos / LineBits, k, sum;
    if (bitpos % WordBits != 0) {
        bm_uint_t x = m_lines[j].words[(bitpos % LineBits / WordBits) * Arity + dimensions];
        if (!(x & (bm_uint_t(1) << bitpos % WordBits))) return 0;
        bm_uint_t y = ~(x >> bitpos % WordBits);
        size_t ctz = fast_ctz(y);
        if (ctz < WordBits - bitpos % WordBits) {
            return ctz;
        }
        assert(ctz == WordBits - bitpos % WordBits);
        k = (bitpos % LineBits / WordBits + 1) * Arity + dimensions;
        sum = ctz;
    }
    else {
        k = (bitpos % LineBits / WordBits) * Arity + dimensions;
        sum = 0;
    }
    size_t len = BitsToLines(m_size[dimensions]) + 1;
    for (; j < len; ++j) {
        for (; k < LineWords * Arity; k += Arity) {
            bm_uint_t y = ~m_lines[j].words[k];
            if (0 == y)
                sum += WordBits;
            else
                return sum + fast_ctz(y);
        }
        k = dimensions;
    }
    return sum;
}

template<size_t Arity>
template<size_t dimensions>
size_t rank_select_mixed_xl_256<Arity>::zero_seq_len_dx(size_t bitpos) const {
    assert(bitpos < m_size[dimensions]);
    size_t j = bitpos / LineBits, k, sum;
    if (bitpos % WordBits != 0) {
        bm_uint_t x = m_lines[j].words[(bitpos % LineBits / WordBits) * Arity + dimensions];
        if (x & (bm_uint_t(1) << bitpos % WordBits)) return 0;
        bm_uint_t y = x >> bitpos % WordBits;
        if (y) {
            return fast_ctz(y);
        }
        k = (bitpos % LineBits / WordBits + 1) * Arity + dimensions;
        sum = WordBits - bitpos % WordBits;
    }
    else {
        k = (bitpos % LineBits / WordBits) * Arity + dimensions;
        sum = 0;
    }
    size_t len = BitsToLines(m_size[dimensions]) + 1;
    for (; j < len; ++j) {
        for (; k < LineWords * Arity; k += Arity) {
            bm_uint_t y = m_lines[j].words[k];
            if (0 == y)
                sum += WordBits;
            else
                return sum + fast_ctz(y);
        }
        k = dimensions;
    }
    return sum;
}

template<size_t Arity>
template<size_t dimensions>
size_t rank_select_mixed_xl_256<Arity>::one_seq_revlen_dx(size_t endpos) const {
    assert(endpos <= m_size[dimensions]);
    size_t j, k, sum;
    if (endpos % WordBits != 0) {
        j = (endpos - 1) / LineBits;
        bm_uint_t x = m_lines[j].words[((endpos - 1) % LineBits / WordBits) * Arity + dimensions];
        if (!(x & (bm_uint_t(1) << (endpos - 1) % WordBits))) return 0;
        bm_uint_t y = ~(x << (WordBits - endpos % WordBits));
        size_t clz = fast_clz(y);
        assert(clz <= endpos % WordBits);
        assert(clz >= 1);
        if (clz < endpos%WordBits) {
            return clz;
        }
        k = (endpos - 1) % LineBits / WordBits - 1;
        sum = clz;
    }
    else {
        if (endpos == 0) return 0;
        j = (endpos - 1) / LineBits;
        k = (endpos - 1) % LineBits / WordBits;
        sum = 0;
    }
    for (; j != size_t(-1); --j) {
        for (; k != size_t(-1); --k) {
            bm_uint_t y = ~m_lines[j].words[k * Arity + dimensions];
            if (0 == y)
                sum += WordBits;
            else
                return sum + fast_clz(y);
        }
        k = 3;
    }
    return sum;
}

template<size_t Arity>
template<size_t dimensions>
size_t rank_select_mixed_xl_256<Arity>::zero_seq_revlen_dx(size_t endpos) const {
    assert(endpos <= m_size[dimensions]);
    size_t j, k, sum;
    if (endpos % WordBits != 0) {
        j = (endpos - 1) / LineBits;
        bm_uint_t x = m_lines[j].words[((endpos - 1) % LineBits / WordBits) * Arity + dimensions];
        if (x & (bm_uint_t(1) << (endpos - 1) % WordBits)) return 0;
        bm_uint_t y = x << (WordBits - endpos % WordBits);
        if (y) {
            return fast_clz(y);
        }
        k = (endpos - 1) % LineBits / WordBits - 1;
        sum = endpos % WordBits;
    }
    else {
        if (endpos == 0) return 0;
        j = (endpos - 1) / LineBits;
        k = (endpos - 1) % LineBits / WordBits;
        sum = 0;
    }
    for (; j != size_t(-1); --j) {
        for (; k != size_t(-1); --k) {
            bm_uint_t y = m_lines[j].words[k * Arity + dimensions];
            if (0 == y)
                sum += WordBits;
            else
                return sum + fast_clz(y);
        }
        k = 3;
    }
    return sum;
}

template<size_t Arity>
template<size_t dimensions>
size_t rank_select_mixed_xl_256<Arity>::select0_dx(size_t Rank0) const {
    assert(m_flags & (1 << (1 + 3 * dimensions)));
    GUARD_MAX_RANK(0[dimensions], Rank0);
    size_t lo, hi;
    if (m_sel0_cache[dimensions]) { // get the very small [lo, hi) range
        lo = m_sel0_cache[dimensions][Rank0 / LineBits];
        hi = m_sel0_cache[dimensions][Rank0 / LineBits + 1];
        //assert(lo < hi);
    }
    else {
        lo = 0;
        hi = (m_size[dimensions] + LineBits + 1) / LineBits;
    }
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = LineBits * mid - m_lines[mid].mixed[dimensions].base;
        if (mid_val <= Rank0) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank0 < LineBits * lo - m_lines[lo].mixed[dimensions].base);
    const auto& xx = m_lines[lo - 1];
    size_t hit = LineBits * (lo - 1) - xx.mixed[dimensions].base;
    size_t index = (lo - 1) * LineBits; // base bit index

    if (Rank0 < hit + 64 * 2 - xx.mixed[dimensions].rlev[2]) {
        if (Rank0 < hit + 64 * 1 - xx.mixed[dimensions].rlev[1]) { // xx.rlev[0] is always 0
            return index + 64 * 0 + UintSelect1(~xx.bit64[dimensions], Rank0 - hit);
        }
        return index + 64 * 1 + UintSelect1(
            ~xx.bit64[1 * Arity + dimensions], Rank0 - (hit + 64 * 1 - xx.mixed[dimensions].rlev[1]));
    }
    if (Rank0 < hit + 64 * 3 - xx.mixed[dimensions].rlev[3]) {
        return index + 64 * 2 + UintSelect1(
            ~xx.bit64[2 * Arity + dimensions], Rank0 - (hit + 64 * 2 - xx.mixed[dimensions].rlev[2]));
    }
    else {
        return index + 64 * 3 + UintSelect1(
            ~xx.bit64[3 * Arity + dimensions], Rank0 - (hit + 64 * 3 - xx.mixed[dimensions].rlev[3]));
    }
}

template<size_t Arity>
template<size_t dimensions>
size_t rank_select_mixed_xl_256<Arity>::select1_dx(size_t Rank1) const {
    assert(m_flags & (1 << (1 + 3 * dimensions)));
    GUARD_MAX_RANK(1[dimensions], Rank1);
    size_t lo, hi;
    if (m_sel1_cache[dimensions]) { // get the very small [lo, hi) range
        lo = m_sel1_cache[dimensions][Rank1 / LineBits];
        hi = m_sel1_cache[dimensions][Rank1 / LineBits + 1];
        //assert(lo < hi);
    }
    else {
        lo = 0;
        hi = (m_size[dimensions] + LineBits + 1) / LineBits;
    }
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        size_t mid_val = m_lines[mid].mixed[dimensions].base;
        if (mid_val <= Rank1) // upper_bound
            lo = mid + 1;
        else
            hi = mid;
    }
    assert(Rank1 < m_lines[lo].mixed[dimensions].base);
    const auto& xx = m_lines[lo - 1];
    size_t hit = xx.mixed[dimensions].base;
    assert(Rank1 >= hit);
    size_t index = (lo - 1) * LineBits; // base bit index
    if (Rank1 < hit + xx.mixed[dimensions].rlev[2]) {
        if (Rank1 < hit + xx.mixed[dimensions].rlev[1]) { // xx.rlev[0] is always 0
            return index + UintSelect1(xx.bit64[dimensions], Rank1 - hit);
        }
        return index + 64 * 1 + UintSelect1(
            xx.bit64[1 * Arity + dimensions], Rank1 - (hit + xx.mixed[dimensions].rlev[1]));
    }
    if (Rank1 < hit + xx.mixed[dimensions].rlev[3]) {
        return index + 64 * 2 + UintSelect1(
            xx.bit64[2 * Arity + dimensions], Rank1 - (hit + xx.mixed[dimensions].rlev[2]));
    }
    else {
        return index + 64 * 3 + UintSelect1(
            xx.bit64[3 * Arity + dimensions], Rank1 - (hit + xx.mixed[dimensions].rlev[3]));
    }
}

template<size_t Index, size_t MyArity, class MyClass>
void instantiate_member_template_aux(MyClass* p) {
    bool b1 = p->m_size[Index] % 299 == 0;
    bool b2 = p->m_size[Index] % 399 == 0;
    p->template bits_range_set0_dx<Index>(0, p->m_size[Index]);
    p->template bits_range_set1_dx<Index>(0, p->m_size[Index]);
    p->template  build_cache_dx<Index>(b1, b2);
    p->template  one_seq_len_dx<Index>(p->m_size[Index] / 2);
    p->template zero_seq_len_dx<Index>(p->m_size[Index] / 2);
    p->template select0_dx<Index>(p->m_size[Index] / 2);
    p->template select1_dx<Index>(p->m_size[Index] / 2);
}
namespace {
    template<size_t Index, size_t Arity, class MyClass>
    struct InstantiateTemplate {
        static void inst(MyClass* p) {
            instantiate_member_template_aux<Index, Arity, MyClass>(p);
            InstantiateTemplate<Index + 1, Arity, MyClass>::inst(p);
        }
    };
    template<size_t Arity, class MyClass>
    struct InstantiateTemplate<Arity, Arity, MyClass> {
        static void inst(MyClass* p) {}
    };
}

template<size_t Arity>
void rank_select_mixed_xl_256<Arity>::instatiate_member_template() {
    InstantiateTemplate<0, Arity, rank_select_mixed_xl_256>::inst(this);
}

template class TERARK_DLL_EXPORT rank_select_mixed_xl_256<2>;
template class TERARK_DLL_EXPORT rank_select_mixed_xl_256<3>;
template class TERARK_DLL_EXPORT rank_select_mixed_xl_256<4>;

// instatiate_member_template does not work for VC, gcc is not tested
// use explicit member template instantiation with TERARK_DLL_EXPORT
#define INSTANTIATE_MEMBER_TEMPLATE(Arity, index) \
template class TERARK_DLL_EXPORT  rank_select_mixed_dimensions<rank_select_mixed_xl_256<Arity>, index>;               \
template TERARK_DLL_EXPORT void   rank_select_mixed_xl_256<Arity>::template bits_range_set0_dx<index>(size_t,size_t); \
template TERARK_DLL_EXPORT void   rank_select_mixed_xl_256<Arity>::template bits_range_set1_dx<index>(size_t,size_t); \
template TERARK_DLL_EXPORT void   rank_select_mixed_xl_256<Arity>::template     build_cache_dx<index>(bool,bool);     \
template TERARK_DLL_EXPORT size_t rank_select_mixed_xl_256<Arity>::template     one_seq_len_dx<index>(size_t) const;  \
template TERARK_DLL_EXPORT size_t rank_select_mixed_xl_256<Arity>::template    zero_seq_len_dx<index>(size_t) const;  \
template TERARK_DLL_EXPORT size_t rank_select_mixed_xl_256<Arity>::template  one_seq_revlen_dx<index>(size_t) const;  \
template TERARK_DLL_EXPORT size_t rank_select_mixed_xl_256<Arity>::template zero_seq_revlen_dx<index>(size_t) const;  \
template TERARK_DLL_EXPORT size_t rank_select_mixed_xl_256<Arity>::template         select0_dx<index>(size_t) const;  \
template TERARK_DLL_EXPORT size_t rank_select_mixed_xl_256<Arity>::template         select1_dx<index>(size_t) const;  \
// End INSTANTIATE_MEMBER_TEMPLATE

INSTANTIATE_MEMBER_TEMPLATE(2, 0)
INSTANTIATE_MEMBER_TEMPLATE(2, 1)

INSTANTIATE_MEMBER_TEMPLATE(3, 0)
INSTANTIATE_MEMBER_TEMPLATE(3, 1)
INSTANTIATE_MEMBER_TEMPLATE(3, 2)

INSTANTIATE_MEMBER_TEMPLATE(4, 0)
INSTANTIATE_MEMBER_TEMPLATE(4, 1)
INSTANTIATE_MEMBER_TEMPLATE(4, 2)
INSTANTIATE_MEMBER_TEMPLATE(4, 3)

} // namespace terark

