public:
template<class _OutIt, class _Cond>
_OutIt copy_if2(_OutIt dest, _Cond cond) {
	assert(!empty());
	value_type x = current_value();
	const key_type eofm = m_max_key;
	assert(comp_key(this->m_extractor(x), eofm));
	do {
		ptrdiff_t n = 0;
		bool bEqual;
		do {
			++n; this->increment();
			value_type y = current_value();
			if (cond(n, x, y))
				*dest++ = x;
			bEqual = !comp_value(x, y);
			x = y;
		} while (bEqual);
	} while (comp_key(this->m_extractor(x), eofm));
	return dest;
}

//! @return next pos of dest
//!
//! @note after calling this function, *this is point to next larger value
//!
template<class _OutIt, class DataExtractor>
_OutIt copy_equal(_OutIt dest, DataExtractor dataEx) {
	assert(!empty());
	const value_type x = current_value();
	const key_type k = this->get_key(x);
	value_type y = x;
	do {
		*dest = dataEx(y); ++dest;
		this->increment();
		y = current_value();
	} while (!comp_key(k, this->get_key(y)));
	return dest;
}

ptrdiff_t skip_equal() {
	assert(!empty());
	key_type k = this->get_key(current_value());
	ptrdiff_t n = 0;
	do {
	   	this->increment();
	   	++n;
	} while (!comp_key(k, this->get_key(current_value())));
	return n;
}

private:
/**
 @note
	-# 每个输入的末尾必须是无穷大元素 eofm
	-# 每个序列不能有重复元素
 */
template<class _OutIt>
_OutIt intersection_n(_OutIt dest) {
	if (empty())
		return dest;
	ptrdiff_t ways = this->total_ways();
	key_type  x = this->get_key(current_value());
	const key_type eofm = m_max_key;
	assert(comp_key(x, eofm));
	do {
		key_type y;
		ptrdiff_t i = 0;
		do {
			this->increment(); ++i;
			y = this->get_key(current_value());
		} while (!comp_key(x, y));
		if (terark_unlikely(i == ways))
			*dest = x, ++dest;
		x = y;
	} while (comp_key(x, eofm));
	return dest;
}

/**
 @note
	-# 如果 MyType 是 LoserTree
	  -# 每个输入的末尾必须是无穷大元素
	-# 每个序列可以有重复元素
 */
template<class _OutIt>
_OutIt intersection_32(_OutIt dest) {
	if (empty())
		return dest;
	const uint32_t mask = 0xFFFFFFFF >> (32-this->total_ways());
	key_type x = this->get_key(current_value());
	const key_type eofm = m_max_key;
	assert(comp_key(x, eofm));
	do {
		uint32_t cur_mask = 0;
		key_type y;
		do {
			cur_mask |= 1 << this->current_way();
			this->increment();
			y = this->get_key(current_value());
		} while (!comp_key(x, y));
		if (mask == cur_mask)
			*dest = x, ++dest;
		x = y;
	} while (comp_key(x, eofm));
	return dest;
}

public:
ptrdiff_t intersection_size() {
	if (empty())
		return 0;
	key_type x = this->get_key(current_value());
	ptrdiff_t n = 0;
	ptrdiff_t ways = this->total_ways();
	const key_type eofm = m_max_key;
	assert(comp_key(x, eofm));
	do {
		key_type y = x;
		ptrdiff_t i = 0;
		do {
			this->increment(); ++i;
			y = this->get_key(current_value());
		} while (!comp_key(x, y));
		if (ways == i)
			++n;
		x = y;
	} while (comp_key(x, eofm));
}

template<class _OutIt>
_OutIt union_set(_OutIt dest) {
	if (empty())
		return dest;
	key_type x = this->get_key(current_value());
	const key_type eofm = m_max_key;
	assert(comp_key(x, eofm));
	do {
		key_type y;
		do {
			this->increment();
			y = this->get_key(current_value());
		} while (!comp_key(x, y));
		*dest++ = x;
		x = y;
	} while (comp_key(x, eofm));
	return dest;
}

ptrdiff_t union_size() {
	if (empty())
		return 0;
	key_type x = this->get_key(current_value());
	ptrdiff_t n = 0;
	const key_type eofm = m_max_key;
	assert(comp_key(x, eofm));
	do {
		key_type y;
		do {
			this->increment();
			y = this->get_key(current_value());
		} while (!comp_key(x, y));
		++n;
		x = y;
	} while (comp_key(x, eofm));
	return n;
}

//! KeyToEqualCountIter::value_type 必须可以从 std::pair<key_type, ptrdiff_t> 隐式转化
//!
template<class KeyToEqualCountIter>
KeyToEqualCountIter key_to_equal_count(KeyToEqualCountIter iter) {
	if (terark_unlikely(empty()))
		return iter;
	key_type x = this->get_key(current_value());
	const key_type eofm = m_max_key;
	assert(comp_key(x, eofm));
	do {
		key_type y;
		ptrdiff_t n = 0;
		do {
			this->increment(); ++n;
			y = this->get_key(current_value());
		} while (!comp_key(x, y));
		*iter = std::pair<key_type, ptrdiff_t>(x, n); ++iter;
		x = y;
	} while (comp_key(x, eofm));
	return iter;
}

//! KeyToEqualCountIter::value_type 必须可以从 std::pair<key_type, ptrdiff_t> 隐式转化
//! @param minEqualCount only output keys which happen minEqualCount times
template<class KeyToEqualCountIter>
KeyToEqualCountIter
key_to_equal_count(KeyToEqualCountIter iter, ptrdiff_t minEqualCount) {
	if (terark_unlikely(empty()))
		return iter;
	key_type x = this->get_key(current_value());
	const key_type eofm = m_max_key;
	assert(comp_key(x, eofm));
	do {
		key_type y;
		ptrdiff_t n = 0;
		do {
			this->increment(); ++n;
			y = this->get_key(current_value());
		} while (!comp_key(x, y));
		if (n >= minEqualCount)
			*iter = std::pair<key_type, ptrdiff_t>(x, n), ++iter;
		x = y;
	} while (comp_key(x, eofm));
	return iter;
}

