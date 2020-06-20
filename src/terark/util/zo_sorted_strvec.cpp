#include "zo_sorted_strvec.hpp"

namespace terark {

class ZoSortedStrVec::Builder::Impl {
public:
    std::unique_ptr<SortedUintVec::Builder> m_suvBuilder;
    valvec<byte_t>         m_strpool;
    Impl(size_t blockUnits) {
        m_suvBuilder.reset(SortedUintVec::createBuilder(blockUnits));
        m_suvBuilder->push_back(0);
    }
};
ZoSortedStrVec::Builder::~Builder() { delete impl; }
ZoSortedStrVec::Builder::Builder() { impl = NULL; }
ZoSortedStrVec::Builder::Builder(size_t blockUnits) {
    impl = new Impl(blockUnits);
}
void ZoSortedStrVec::Builder::init(size_t blockUnits) {
    if  (impl) {
        THROW_STD(invalid_argument, "this Builder is already initialized");
    }
    impl = new Impl(blockUnits);
}
void ZoSortedStrVec::Builder::reserve_strpool(size_t cap) {
    assert(NULL != impl);
    impl->m_strpool.reserve(cap);
}
void ZoSortedStrVec::Builder::push_back(fstring str) {
    assert(NULL != impl);
    impl->m_strpool.append(str);
    impl->m_suvBuilder->push_back(impl->m_strpool.size());
}
void ZoSortedStrVec::Builder::push_offset(size_t offset) {
    assert(NULL != impl);
    impl->m_suvBuilder->push_back(impl->m_strpool.size());
}
void ZoSortedStrVec::Builder::finish(ZoSortedStrVec* strVec) {
    assert(NULL != impl);
    impl->m_suvBuilder->finish(&strVec->m_offsets);
    size_t strNum = strVec->m_offsets.size() - 1;
    size_t poolsize = strVec->m_offsets.get(strNum);
    if (impl->m_strpool.size() == poolsize) {
        strVec->m_strpool.swap(impl->m_strpool);
    }
    else if (impl->m_strpool.size() != 0) {
        THROW_STD(logic_error, "May be mixed use of push_back and push_offset");
    }
    delete impl;
    impl = NULL;
}

ZoSortedStrVec::ZoSortedStrVec() {
    m_strpool_mem_type = MemType::Malloc;
}
ZoSortedStrVec::~ZoSortedStrVec() {
    m_strpool.risk_destroy(m_strpool_mem_type);
}

void ZoSortedStrVec::swap(ZoSortedStrVec& y) {
    m_offsets.swap(y.m_offsets);
    m_strpool.swap(y.m_strpool);
    std::swap(m_strpool_mem_type, y.m_strpool_mem_type);
}

void ZoSortedStrVec::sort() {
    THROW_STD(invalid_argument, "This method is not supported");
}

void ZoSortedStrVec::clear() {
    m_offsets.clear();
    m_strpool.risk_destroy(m_strpool_mem_type);
}

#define ZoSortedStrVec_SEARCH_INDEX

template<size_t RangeSize, class SearchFunc>
static size_t
search_small_range(const SortedUintVec& suv, size_t lo, size_t hi, SearchFunc sf) {
    size_t vals[RangeSize];
    size_t log2 = suv.log2_block_units();
    size_t blockLen = size_t(1) << log2;
    size_t mask = blockLen - 1;
    size_t blockIdx = lo >> log2;
    size_t blockNum = (((hi+mask)&~mask) - (lo&~mask)) >> log2;
    assert(blockNum <= RangeSize >> log2);
    for (size_t i = 0; i < blockNum; ++i) {
        suv.get_block(blockIdx + i, vals + (i << log2));
    }
    return lo + sf(vals + (lo & mask), hi-lo);
}

size_t ZoSortedStrVec::lower_bound_by_offset(size_t offset) const {
    return m_offsets.lower_bound(0, m_offsets.size(), offset);
}

size_t ZoSortedStrVec::upper_bound_by_offset(size_t offset) const {
    return m_offsets.upper_bound(0, m_offsets.size(), offset);
}

size_t
ZoSortedStrVec::upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch)
const {
    assert(m_offsets.size() >= 1);
    assert(lo < hi);
    assert(hi <= m_offsets.size()-1);
    byte_t const* s = m_strpool.data();
#ifdef ZoSortedStrVec_SEARCH_INDEX
    size_t log2 = m_offsets.log2_block_units();
    size_t blockUnits = size_t(1) << log2;
    assert(blockUnits <= 128);
    size_t mask = blockUnits - 1;
    size_t loBlk = lo >> log2;
    size_t block[128];
    m_offsets.get_block(loBlk, block);
#if !defined(NDEBUG)
    const byte_t kh = s[block[lo & mask] + pos];
    assert(kh == ch);
#endif
    auto getPosChar = [=](size_t offset){ return s[offset + pos];};
    if ((hi-1) >> log2 == loBlk) { // in same block
        const size_t bound = ((hi-1) & mask) + 1;
        return (lo & ~mask) +
            upper_bound_ex_n(block, lo & mask, bound, ch, getPosChar);
    }
    else {
        size_t inBlockUpp =
            upper_bound_ex_n(block, lo & mask, blockUnits, ch, getPosChar);
        if (inBlockUpp < blockUnits)
            return (lo & ~mask) + inBlockUpp;
        loBlk++;
        const void*    indexBase = m_offsets.get_index_base();
        const size_t sampleWidth = m_offsets.get_sample_width();
        const size_t  indexWidth = m_offsets.get_index_width();
        const byte_t  ch = s[m_offsets[lo] + pos];
        size_t hiBlk = (hi + mask) >> log2;
        while (loBlk < hiBlk) {
            size_t mid = (loBlk + hiBlk) / 2;
            size_t offset = SortedUintVec::s_get_block_min_val(
                indexBase, sampleWidth, indexWidth, mid);
            if (s[offset + pos] <= ch)
                loBlk = mid + 1;
            else
                hiBlk = mid;
        }
        assert(loBlk > 0);
        loBlk--;
        if (terark_unlikely(lo >> log2 == loBlk)) {
            return hiBlk << log2;
        }
        m_offsets.get_block(loBlk, block);
        size_t bound = (hi < hiBlk << log2) ? ((hi-1) & mask) + 1 : blockUnits;
        return (loBlk << log2) +
            upper_bound_ex_0(block, bound, ch, getPosChar);
    }
#else
    byte_t const ch = s[m_offsets[lo] + pos];
    while ((lo & ~size_t(127)) + 256 < ((hi + 127)& ~size_t(127))) {
        size_t mid = (lo + hi) / 2;
        size_t BegEnd[2];  m_offsets.get2(mid, BegEnd);
        assert(BegEnd[0] <= BegEnd[1]);
        assert(pos < BegEnd[1] - BegEnd[0]);
        if (s[BegEnd[0] + pos] <= ch)
            lo = mid + 1;
        else
            hi = mid;
    }
    return search_small_range<256>(m_offsets, lo, hi,
        [=](const size_t* offsets, size_t num) {
            return upper_bound_ex_0(offsets, num, ch,
                [=](size_t offset){ return s[offset + pos];});
        });
#endif
}

size_t ZoSortedStrVec::lower_bound(fstring key) const {
    size_t lo = 0, hi = m_offsets.size()-1;
    auto   strpool = m_strpool.data();
    while ((lo & ~size_t(127)) + 256 < ((hi + 127)& ~size_t(127))) {
        size_t mid = (lo + hi) / 2;
        size_t BegEnd[2];  m_offsets.get2(mid, BegEnd);
        assert(BegEnd[0] <= BegEnd[1]);
        fstring mid_val(strpool + BegEnd[0], BegEnd[1]-BegEnd[0]);
        if (mid_val < key)
            lo = mid + 1;
        else
            hi = mid;
    }
    return search_small_range<256>(m_offsets, lo, hi,
        [=](size_t* offsets, size_t num) {
            return lower_bound_ex_0(offsets, num, key,
                [=](size_t& offset){
                    return fstring(strpool + offset, (&offset)[1] - offset);
                });
        });
}

size_t ZoSortedStrVec::upper_bound(fstring key) const {
    size_t lo = 0, hi = m_offsets.size()-1;
    auto   strpool = m_strpool.data();
    while ((lo & ~size_t(127)) + 256 < ((hi + 127)& ~size_t(127))) {
        size_t mid = (lo + hi) / 2;
        size_t BegEnd[2];  m_offsets.get2(mid, BegEnd);
        assert(BegEnd[0] <= BegEnd[1]);
        fstring mid_val(strpool + BegEnd[0], BegEnd[1]-BegEnd[0]);
        if (mid_val <= key)
            lo = mid + 1;
        else
            hi = mid;
    }
    return search_small_range<256>(m_offsets, lo, hi,
        [=](size_t* offsets, size_t num) {
            return upper_bound_ex_0(offsets, num, key,
                [=](size_t& offset){
                    return fstring(strpool + offset, (&offset)[1] - offset);
                });
        });
}

size_t ZoSortedStrVec::max_strlen() const {
    size_t log2 = m_offsets.log2_block_units();
    size_t bsize = size_t(1) << log2;
    size_t nb = pow2_align_up(size(), bsize) >> log2;
    size_t block[128];
    size_t lastVal = 0;
    size_t maxlen = 0;
    for (size_t i = 0; i < nb; ++i) {
        m_offsets.get_block(i, block);
        size_t x = block[0] - lastVal;
        if (maxlen < x)
            maxlen = x;
        for (size_t j = 1; j < bsize; ++j) {
            x = block[j] - block[j-1];
            if (maxlen < x)
                maxlen = x;
        }
        lastVal = block[bsize-1];
    }
    return maxlen;
}

void ZoSortedStrVecWithBuilder::init(size_t blockUnits) {
    m_builder.init(blockUnits);
}

void ZoSortedStrVecWithBuilder::reserve(size_t strNum, size_t maxStrPool) {
    TERARK_UNUSED_VAR(strNum);
    m_builder.reserve_strpool(maxStrPool);
}

void ZoSortedStrVecWithBuilder::finish() {
    m_builder.finish(this);
}

void ZoSortedStrVecWithBuilder::push_back(fstring str) {
    m_builder.push_back(str);
}

void ZoSortedStrVecWithBuilder::push_offset(size_t offset) {
    m_builder.push_offset(offset);
}

} // namespace terark

