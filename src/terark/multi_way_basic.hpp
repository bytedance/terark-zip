
private:
void operator++(int); //!< disabled

public:
const value_type& operator* () const { return  current_value(); }
const value_type* operator->() const { return &current_value(); }
MyType& operator++() { this->increment(); return *this; }

public:
bool comp_value(const value_type& x, const value_type& y) const {
	return this->m_comp(this->get_key(x), this->get_key(y));
}

bool comp_key(const key_type& x, const key_type& y) const {
	return this->m_comp(x, y);
}

/**
 @brief 求多个集合的和集，相当于合并多个有序序列

 这个操作对序列的要求有所放宽，允许每个序列中的元素可以重复
 @see intersection
 */
template<class _OutIt>
_OutIt merge(_OutIt dest) {
	for (; !empty(); this->increment(), ++dest)
		*dest = current_value();
	return dest;
}
template<class _OutIt>
_OutIt copy(_OutIt dest) { return merge(dest); }

template<class _OutIt, class _Cond>
_OutIt copy_if(_OutIt dest, _Cond cond) {
	while (!empty()) {
		value_type x = current_value();
		if (cond(x))
			*dest = x, ++dest;
	}
	return dest;
}

template<class _OutIt>
_OutIt copy_equal(_OutIt dest) {
	return copy_equal(dest, boost::multi_index::identity<value_type>());
}

/**
 @brief 求多个集合的交集

 @param dest  将结果拷贝到这里

 @note
	-# 如果 MyType 是 LoserTree
	  -# 每个输入的末尾必须是无穷大元素
 */
template<class _OutIt>
_OutIt intersection(_OutIt dest) {
	assert(!empty());
	if (this->total_ways() <= 32)
		return intersection_32(dest);
	else
		return intersection_n(dest);
}

template<class _OutIt>
_OutIt unique(_OutIt dest) { return union_set(dest); }

