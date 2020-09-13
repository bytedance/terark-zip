#include "rev_ord_strvec.hpp"
#include <terark/io/DataIO_Basic.hpp>

// 2020-06-21 00:16 -- prefetch has no observable improve
//#define StrVec_EnablePrefetch
#if defined(StrVec_EnablePrefetch)
	#define StrVec_prefetch(cond, ptr) do if (cond) _mm_prefetch((const char*)ptr, _MM_HINT_T0); while (0)
#else
	#define StrVec_prefetch(cond, ptr)
#endif
namespace terark {

static inline
bool str_less(const void* xp, size_t xn, const void* yp, size_t yn) {
	ptrdiff_t n = std::min(xn, yn);
	int ret = memcmp(xp, yp, n);
	if (ret)
		return ret < 0;
	else
		return xn < yn;
}

///////////////////////////////////////////////////////////////////////////////

FixedRevStrVec::~FixedRevStrVec() {
	m_strpool.risk_destroy(m_strpool_mem_type);
}

void FixedRevStrVec::reserve(size_t strNum, size_t maxStrPool) {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(m_fixlen * strNum == maxStrPool);
    m_strpool.reserve(maxStrPool);
}

void FixedRevStrVec::shrink_to_fit() {
    assert(m_fixlen * m_size == m_strpool.size());
    m_strpool.shrink_to_fit();
}

void FixedRevStrVec::risk_release_ownership() {
    m_strpool.risk_release_ownership();
    m_size = 0;
}

void FixedRevStrVec::swap(FixedRevStrVec& y) {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(y.m_fixlen * y.m_size == y.m_strpool.size());
    std::swap(m_fixlen, y.m_fixlen);
    std::swap(m_size  , y.m_size);
    m_strpool.swap(y.m_strpool);
	std::swap(m_strpool_mem_type, y.m_strpool_mem_type);
}

void FixedRevStrVec::push_back(fstring str) {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(str.size() == m_fixlen);
    if (str.size() != m_fixlen) {
        THROW_STD(length_error,
            "expected: %zd, real: %zd", m_fixlen, str.size());
    }
    m_strpool.append(str.data(), str.size());
    m_size++;
}

void FixedRevStrVec::pop_back() {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(m_strpool.size() > m_fixlen);
    m_strpool.pop_n(m_fixlen);
    m_size--;
}

/*
void FixedRevStrVec::back_append(fstring str) {
}
void FixedRevStrVec::back_shrink(size_t nShrink) {
}
void FixedRevStrVec::back_grow_no_init(size_t nGrow) {
}
*/

void FixedRevStrVec::reverse_keys() {
    assert(m_fixlen > 0);
    assert(m_fixlen * m_size == m_strpool.size());
    byte_t* beg = m_strpool.begin();
    byte_t* end = m_strpool.end();
    size_t  fixlen = m_fixlen;
    while (beg < end) {
    //  std::reverse(beg, beg + fixlen);
        byte_t* lo = beg;
        byte_t* hi = beg + fixlen;
        while (lo < --hi) {
            byte_t tmp = *lo;
            *lo = *hi;
            *hi = tmp;
            ++lo;
        }
        beg += fixlen;
    }
}

#if defined(_MSC_VER) || defined(__APPLE__)
static int CmpFixRevStr(void* ctx, const void* x, const void* y)
#else
static int CmpFixRevStr(const void* x, const void* y, void* ctx)
#endif
{
    size_t fixlen = (size_t)(ctx);
    return -memcmp(x, y, fixlen);
}

void FixedRevStrVec::sort() {
    assert(m_fixlen * m_size == m_strpool.size());
    sort_raw(m_strpool.data(), m_size, m_fixlen);
}

void FixedRevStrVec::sort_raw(void* base, size_t num, size_t fixlen) {
#ifdef _MSC_VER
    #define QSortCtx qsort_s
#elif defined(__APPLE__)
    auto QSortCtx = [](void *base, size_t size, size_t nmemb,
    		           int (*compar)(void *, const void *, const void *),
					   void *thunk) {
		qsort_r(base, nmemb, size, thunk, compar);
    };
#else
    #define QSortCtx qsort_r
#endif
    QSortCtx(base, num, fixlen, CmpFixRevStr, (void*)(fixlen));
}

void FixedRevStrVec::clear() {
//  assert is not needed
//  assert(m_fixlen * m_size == m_strpool.size());
    m_size = 0;
    m_strpool.risk_destroy(m_strpool_mem_type);
}

size_t FixedRevStrVec::lower_bound_by_offset(size_t offset) const {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(offset <= m_strpool.size());
    size_t  fixlen = m_fixlen;
    size_t  idx = offset / fixlen;
    idx = m_size - 1 - idx;
    if (offset % fixlen == 0) {
        return idx;
    }
    else {
        return idx + 1;
    }
}

size_t FixedRevStrVec::upper_bound_by_offset(size_t offset) const {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(offset <= m_strpool.size());
    size_t  fixlen = m_fixlen;
    size_t  idx = m_size - 1 - (offset / fixlen);
    return  idx + 1;
}

size_t FixedRevStrVec::upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const {
    assert(m_fixlen * m_size == m_strpool.size());
    assert(lo < hi);
    assert(hi <= m_size);
    assert(pos < m_fixlen);
    size_t  const fixlen = m_fixlen;
    size_t  const  imax = m_size - 1;
    byte_t  const* pool = m_strpool.data();
#if !defined(NDEBUG)
    const byte_t kh = pool[fixlen * (imax - lo) + pos];
    assert(kh == ch);
#endif
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        assert(pos < fixlen);
		StrVec_prefetch(true, pool + fixlen*(imax - (lo+mid)/2));
		StrVec_prefetch(true, pool + fixlen*(imax - (hi+mid)/2));
        if (pool[fixlen * (imax - mid) + pos] <= ch)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

terark_flatten
size_t FixedRevStrVec::lower_bound(size_t lo, size_t hi, fstring key) const {
	assert(m_fixlen * m_size == m_strpool.size());
	assert(lo <= hi);
	assert(hi <= m_size);
	auto fixlen = m_fixlen;
	auto data = m_strpool.data();
    auto imax = m_size - 1;
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_pos = fixlen * (imax - mid_idx);
		if (fstring(data + mid_pos, fixlen) < key)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

terark_flatten
size_t FixedRevStrVec::upper_bound(size_t lo, size_t hi, fstring key) const {
	assert(m_fixlen * m_size == m_strpool.size());
	assert(lo <= hi);
	assert(hi <= m_size);
	auto fixlen = m_fixlen;
	auto data = m_strpool.data();
    auto imax = m_size - 1;
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_pos = fixlen * (imax - mid_idx);
		if (fstring(data + mid_pos, fixlen) <= key)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

terark_flatten
static size_t FixedRev_lower_bound_slow(const FixedRevStrVec* sv,
                                     size_t lo, size_t hi, const void* key) {
	assert(sv->m_fixlen * sv->m_size == sv->m_strpool.size());
	assert(lo <= hi);
	assert(hi <= sv->m_size);
	auto fixlen = sv->m_fixlen;
	auto data = sv->m_strpool.data();
    auto imax = sv->m_size - 1;
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_pos = fixlen * (imax - mid_idx);
		if (memcmp(data + mid_pos, key, fixlen) < 0)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

terark_flatten
static size_t FixedRev_upper_bound_slow(const FixedRevStrVec* sv,
                                     size_t lo, size_t hi, const void* key) {
	assert(sv->m_fixlen * sv->m_size == sv->m_strpool.size());
	assert(lo <= hi);
	assert(hi <= sv->m_size);
	auto fixlen = sv->m_fixlen;
	auto data = sv->m_strpool.data();
    auto imax = sv->m_size - 1;
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_pos = fixlen * (imax - mid_idx);
		if (memcmp(data + mid_pos, key, fixlen) <= 0)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}


template<class Uint>
terark_flatten
static size_t FixedRev_lower_bound(const FixedRevStrVec* sv,
                                size_t lo, size_t hi, const void* key) {
    assert(sizeof(Uint) == sv->m_fixlen);
	assert(sv->m_fixlen * sv->m_size == sv->m_strpool.size());
	assert(lo <= hi);
	assert(hi <= sv->m_size);
	assert(size_t(sv->m_strpool.data()) % sizeof(Uint) == 0);
	Uint ukey = unaligned_load<Uint>(key);
	auto data = (const Uint*)sv->m_strpool.data();
    auto imax = sv->m_size - 1;
	BYTE_SWAP_IF_LITTLE_ENDIAN(ukey);
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		Uint   mid_val = data[imax - mid_idx];
		BYTE_SWAP_IF_LITTLE_ENDIAN(mid_val);
		if (mid_val < ukey)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

template<class Uint>
terark_flatten
static size_t FixedRev_upper_bound(const FixedRevStrVec* sv,
                                size_t lo, size_t hi, const void* key) {
  assert(sizeof(Uint) == sv->m_fixlen);
	assert(sv->m_fixlen * sv->m_size == sv->m_strpool.size());
	assert(lo <= hi);
	assert(hi <= sv->m_size);
	assert(size_t(sv->m_strpool.data()) % sizeof(Uint) == 0);
	Uint ukey = unaligned_load<Uint>(key);
	auto data = (const Uint*)sv->m_strpool.data();
    auto imax = sv->m_size - 1;
	BYTE_SWAP_IF_LITTLE_ENDIAN(ukey);
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		Uint   mid_val = data[imax - mid_idx];
		BYTE_SWAP_IF_LITTLE_ENDIAN(mid_val);
		if (mid_val <= ukey)
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

FixedRevStrVec::FixedRevStrVec(size_t fixlen) {
    m_lower_bound_fixed = &FixedRev_lower_bound_slow;
    m_upper_bound_fixed = &FixedRev_upper_bound_slow;
    m_fixlen = fixlen;
    m_size = 0;
    m_strpool_mem_type = MemType::Malloc;
}

void FixedRevStrVec::optimize_func() {
    switch (m_fixlen) {
    default:
        m_lower_bound_fixed = &FixedRev_lower_bound_slow;
        m_upper_bound_fixed = &FixedRev_upper_bound_slow;
        break;
#define SetFuncPtr(Uint) \
    case sizeof(Uint): \
        m_lower_bound_fixed = &FixedRev_lower_bound<Uint>; \
        m_upper_bound_fixed = &FixedRev_upper_bound<Uint>; \
        break
//----------------------------------------------------------
        SetFuncPtr(uint08_t);
        SetFuncPtr(uint16_t);
        SetFuncPtr(uint32_t);
        SetFuncPtr(uint64_t);
#undef  SetFuncPtr
    }
}

///////////////////////////////////////////////////////////////////////////////

RevOrdStrVec::RevOrdStrVec() {
	m_offsets_mem_type = MemType::Malloc;
	m_strpool_mem_type = MemType::Malloc;
}
RevOrdStrVec::~RevOrdStrVec() {
	m_offsets.risk_destroy(m_offsets_mem_type);
	m_strpool.risk_destroy(m_strpool_mem_type);
}

void RevOrdStrVec::reserve(size_t strNum, size_t maxStrPool) {
    m_strpool.reserve(maxStrPool);
    m_offsets.resize_with_wire_max_val(strNum+1, maxStrPool);
    m_offsets.resize(0);
}

void RevOrdStrVec::shrink_to_fit() {
    if (0 == m_offsets.size()) {
        if (0 == m_offsets.uintbits()) {
            m_offsets.resize_with_wire_max_val(0, 1ul);
        }
        m_offsets.push_back(0);
    }
    m_strpool.shrink_to_fit();
    m_offsets.shrink_to_fit();
}

void RevOrdStrVec::swap(RevOrdStrVec& y) {
    m_offsets.swap(y.m_offsets);
    m_strpool.swap(y.m_strpool);
	std::swap(m_offsets_mem_type, y.m_offsets_mem_type);
	std::swap(m_strpool_mem_type, y.m_strpool_mem_type);
}

void RevOrdStrVec::push_back(fstring str) {
    if (terark_unlikely(m_strpool.size() + str.size() > m_offsets.uintmask())) {
        THROW_STD(length_error,
            "exceeding offset domain(bits=%zd)", m_offsets.uintbits());
    }
    if (terark_unlikely(m_offsets.size() == 0)) {
        if (m_offsets.uintbits() == 0) {
            THROW_STD(invalid_argument,
                "m_offsets.uintbits() == 0, Object is not initialized");
        }
        m_offsets.push_back(0);
    }
    m_strpool.append(str.data(), str.size());
    m_offsets.push_back(m_strpool.size());
}

void RevOrdStrVec::pop_back() {
    assert(m_offsets.size() > 2);
    m_strpool.risk_set_size(m_offsets.get(m_offsets.size()-2));
    m_offsets.resize(m_offsets.size()-1);
}

void RevOrdStrVec::back_append(fstring str) {
    if (m_strpool.size() + str.size() > m_offsets.uintmask()) {
        THROW_STD(length_error,
            "exceeding offset domain(bits=%zd)", m_offsets.uintbits());
    }
    m_strpool.append(str.data(), str.size());
    m_offsets.set_wire(m_offsets.size()-1, m_strpool.size());
}

void RevOrdStrVec::back_shrink(size_t nShrink) {
    size_t lastIdx = m_offsets.size() - 2;
    if (this->nth_size(lastIdx) < nShrink) {
        THROW_STD(length_error,
            "nShrink = %zd, strlen = %zd", nShrink, nth_size(lastIdx));
    }
    m_strpool.pop_n(nShrink);
    m_offsets.set_wire(lastIdx+1, m_strpool.size());
}

void RevOrdStrVec::back_grow_no_init(size_t nGrow) {
    if (m_strpool.size() + nGrow > m_offsets.uintmask()) {
        THROW_STD(length_error,
            "exceeding offset domain(bits=%zd)", m_offsets.uintbits());
    }
    assert(m_offsets.size() >= 2);
    m_strpool.resize_no_init(m_strpool.size() + nGrow);
    m_offsets.set_wire(m_offsets.size()-1, m_strpool.size());
}

void RevOrdStrVec::reverse_keys() {
    byte_t* beg = m_strpool.begin();
    size_t  offset = 0;
    for(size_t i = 0, n = m_offsets.size()-1; i < n; ++i) {
        size_t endpos = m_offsets[i+1];
    //  std::reverse(beg, beg + fixlen);
        byte_t* lo = beg + offset;
        byte_t* hi = beg + endpos;
        while (lo < --hi) {
            byte_t tmp = *lo;
            *lo = *hi;
            *hi = tmp;
            ++lo;
        }
        offset = endpos;
    }
}

void RevOrdStrVec::sort() {
    THROW_STD(invalid_argument, "This method is not supported");
}

void RevOrdStrVec::clear() {
    m_offsets.risk_destroy(m_offsets_mem_type);
    m_strpool.risk_destroy(m_strpool_mem_type);
}

terark_flatten
size_t RevOrdStrVec::lower_bound_by_offset(size_t offset) const {
    return lower_bound_0<const UintVecMin0&>(m_offsets, m_offsets.size()-1, offset);
}

terark_flatten
size_t RevOrdStrVec::upper_bound_by_offset(size_t offset) const {
    return upper_bound_0<const UintVecMin0&>(m_offsets, m_offsets.size()-1, offset);
}

terark_flatten
size_t RevOrdStrVec::upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const {
    assert(m_offsets.size() >= 2); // RevOrdStrVec must not be empty
    assert(lo < hi);
    assert(hi <= m_offsets.size()-1);
    const byte_t* data = m_offsets.data();
    const size_t  bits = m_offsets.uintbits();
    const size_t  mask = m_offsets.uintmask();
	const size_t  imax = m_offsets.size() - 2;
    const byte_t* pool = m_strpool.data();
#if !defined(NDEBUG)
    const byte_t kh = pool[UintVecMin0::fast_get(data, bits, mask, imax - lo) + pos];
    assert(kh == ch);
    for (size_t i = lo; i < hi; ++i) {
        fstring s = (*this)[i];
        TERARK_ASSERT_LT(pos, s.size());
    }
#endif
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        TERARK_ASSERT_LT(pos, this->nth_size(mid));
        size_t offset = UintVecMin0::fast_get(data, bits, mask, imax - mid);
        if (pool[offset + pos] <= ch)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

terark_flatten
size_t RevOrdStrVec::lower_bound(size_t start, size_t end, fstring key) const {
	assert(start <= end);
	assert(end <= m_offsets.size()-1);
	const auto pool = m_strpool.data();
	const auto data = m_offsets.data();
	const auto imax = m_offsets.size() - 2;
	const auto bits = m_offsets.uintbits();
	const auto mask = m_offsets.uintmask();
	size_t lo = start, hi = end;
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_rev = imax - mid_idx;
		size_t mid_beg = UintVecMin0::fast_get(data, bits, mask, mid_rev);
		size_t mid_end = UintVecMin0::fast_get(data, bits, mask, mid_rev+1);
		size_t mid_len = mid_end - mid_beg;
		TERARK_ASSERT_LE(mid_beg, mid_end);
		if (str_less(pool + mid_beg, mid_len, key.p, key.n))
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

terark_flatten
size_t RevOrdStrVec::upper_bound(size_t start, size_t end, fstring key) const {
	assert(start <= end);
	assert(end <= m_offsets.size()-1);
	const auto pool = m_strpool.data();
	const auto data = m_offsets.data();
	const auto imax = m_offsets.size() - 2;
	const auto bits = m_offsets.uintbits();
	const auto mask = m_offsets.uintmask();
	size_t lo = start, hi = end;
	while (lo < hi) {
		size_t mid_idx = (lo + hi) / 2;
		size_t mid_rev = imax - mid_idx;
		size_t mid_beg = UintVecMin0::fast_get(data, bits, mask, mid_rev);
		size_t mid_end = UintVecMin0::fast_get(data, bits, mask, mid_rev+1);
		size_t mid_len = mid_end - mid_beg;
		TERARK_ASSERT_LE(mid_beg, mid_end);
		if (!str_less(key.p, key.n, pool + mid_beg, mid_len))
			lo = mid_idx + 1;
		else
			hi = mid_idx;
	}
	return lo;
}

size_t RevOrdStrVec::max_strlen() const {
    const auto data = m_offsets.data();
    const auto size = m_offsets.size();
    const auto bits = m_offsets.uintbits();
    const auto mask = m_offsets.uintmask();
    size_t maxlen = 0;
    for(size_t i = 1; i < size; ++i) {
        size_t s = m_offsets.fast_get(data, bits, mask, i-1);
        size_t t = m_offsets.fast_get(data, bits, mask, i-0);
        size_t x = t - s;
        if (maxlen < x)
            maxlen = x;
    }
    return maxlen;
}

/////////////////////////////////////////////////////////////////////////////

template<class UintXX>
RevOrdStrVecUintTpl<UintXX>::RevOrdStrVecUintTpl(size_t delim_len) {
	m_offsets.push_back(0);
	m_delim_len = delim_len;
	m_offsets_mem_type = MemType::Malloc;
	m_strpool_mem_type = MemType::Malloc;
}

template<class UintXX>
RevOrdStrVecUintTpl<UintXX>::~RevOrdStrVecUintTpl() {
	m_offsets.risk_destroy(m_offsets_mem_type);
	m_strpool.risk_destroy(m_strpool_mem_type);
}

template<class UintXX>
void RevOrdStrVecUintTpl<UintXX>::reserve(size_t strNum, size_t maxStrPool) {
    m_strpool.reserve(maxStrPool + m_delim_len*strNum);
    m_offsets.reserve(strNum+1);
}

template<class UintXX>
void RevOrdStrVecUintTpl<UintXX>::shrink_to_fit() {
    m_strpool.shrink_to_fit();
    m_offsets.shrink_to_fit();
}

template<class UintXX>
void RevOrdStrVecUintTpl<UintXX>::swap(RevOrdStrVecUintTpl& y) {
    m_offsets.swap(y.m_offsets);
    m_strpool.swap(y.m_strpool);
	std::swap(m_delim_len, y.m_delim_len);
	std::swap(m_offsets_mem_type, y.m_offsets_mem_type);
	std::swap(m_strpool_mem_type, y.m_strpool_mem_type);
}

template<class UintXX>
void RevOrdStrVecUintTpl<UintXX>::push_back(fstring str) {
    m_strpool.append(str.data(), str.size());
    m_offsets.push_back(m_strpool.size());
	m_offsets.push_n(m_delim_len, '\0');
}

template<class UintXX>
void RevOrdStrVecUintTpl<UintXX>::pop_back() {
	size_t osize = m_offsets.size();
    assert(osize > 2);
    m_strpool.risk_set_size(m_offsets[osize-2]);
    m_offsets.risk_set_size(osize-1);
}

template<class UintXX>
void RevOrdStrVecUintTpl<UintXX>::back_grow_no_init(size_t nGrow) {
    assert(m_offsets.size() >= 2);
	size_t slen = m_strpool.size() + nGrow;
    m_strpool.resize_no_init(slen);
    m_offsets.back() = slen;
}
template<class UintXX>
void RevOrdStrVecUintTpl<UintXX>::reverse_keys() {
    byte_t* beg = m_strpool.begin();
    size_t  offset = 0;
    for(size_t i = 0, n = m_offsets.size()-1; i < n; ++i) {
        size_t endpos = m_offsets[i+1];
    //  std::reverse(beg, beg + fixlen);
        byte_t* lo = beg + offset;
        byte_t* hi = beg + endpos - m_delim_len;
        while (lo < --hi) {
            byte_t tmp = *lo;
            *lo = *hi;
            *hi = tmp;
            ++lo;
        }
        offset = endpos;
    }
}

template<class UintXX>
void RevOrdStrVecUintTpl<UintXX>::sort() {
    THROW_STD(invalid_argument, "This method is not supported");
}

template<class UintXX>
void RevOrdStrVecUintTpl<UintXX>::clear() {
    m_offsets.risk_destroy(m_offsets_mem_type);
    m_strpool.risk_destroy(m_strpool_mem_type);
}

template<class UintXX>
terark_flatten
size_t RevOrdStrVecUintTpl<UintXX>::lower_bound_by_offset(size_t offset) const {
    return lower_bound_0(m_offsets.begin(), m_offsets.size()-1, offset);
}

template<class UintXX>
terark_flatten
size_t RevOrdStrVecUintTpl<UintXX>::upper_bound_by_offset(size_t offset) const {
    return upper_bound_0(m_offsets.begin(), m_offsets.size()-1, offset);
}

template<class UintXX>
terark_flatten
size_t RevOrdStrVecUintTpl<UintXX>::upper_bound_at_pos(size_t lo, size_t hi, size_t pos, byte_t ch) const {
    TERARK_ASSERT_GE(m_offsets.size(), 2);
    TERARK_ASSERT_LE(lo, hi);
    TERARK_ASSERT_LE(hi, m_offsets.size()-1);
    const UintXX* data = m_offsets.data();
    const byte_t* pool = m_strpool.data();
    const size_t  imax = m_offsets.size() - 2;
#if !defined(NDEBUG)
    const byte_t kh = pool[data[lo] + pos];
    assert(kh == ch);
    for (size_t i = lo; i < hi; ++i) {
        fstring s = (*this)[i];
        TERARK_ASSERT_LT(pos, s.size());
    }
#endif
    while (lo < hi) {
        size_t mid_idx = (lo + hi) / 2;
        size_t mid_rev = imax - mid_idx;
        TERARK_ASSERT_LT(pos, this->nth_size(mid_idx));
		StrVec_prefetch(true, data + (lo + mid_rev)/2);
		StrVec_prefetch(true, data + (hi + mid_rev)/2);
        size_t offset = data[mid_rev];
        if (pool[offset + pos] <= ch)
            lo = mid_idx + 1;
        else
            hi = mid_idx;
    }
    return lo;
}

template<class UintXX>
terark_flatten
size_t RevOrdStrVecUintTpl<UintXX>::lower_bound(size_t start, size_t end, fstring key) const {
    TERARK_ASSERT_LE(start, end);
    TERARK_ASSERT_LE(end, m_offsets.size()-1);
    return lower_bound_n<const RevOrdStrVecUintTpl<UintXX>&>(*this, start, end, key);
}

template<class UintXX>
terark_flatten
size_t RevOrdStrVecUintTpl<UintXX>::upper_bound(size_t start, size_t end, fstring key) const {
    TERARK_ASSERT_LE(start, end);
    TERARK_ASSERT_LE(end, m_offsets.size()-1);
    return upper_bound_n<const RevOrdStrVecUintTpl<UintXX>&>(*this, start, end, key);
}

template<class UintXX>
terark_flatten
size_t RevOrdStrVecUintTpl<UintXX>::max_strlen() const {
	const auto offsets = m_offsets.data();
    const auto size = m_offsets.size();
    size_t maxlen = 0;
    for(size_t i = 1; i < size; ++i) {
        size_t s = offsets[i-1];
        size_t t = offsets[i-0];
        size_t x = t - s;
        if (maxlen < x)
            maxlen = x;
    }
    return maxlen - m_delim_len;
}

template class RevOrdStrVecUintTpl<uint32_t>;
template class RevOrdStrVecUintTpl<uint64_t>;


} // namespace terark

