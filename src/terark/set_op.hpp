/* vim: set tabstop=4 : */
/********************************************************************
	@file set_op.hpp
	@brief set operations

	@date	2006-10-23 14:02
	@author	leipeng
	@{
*********************************************************************/
#ifndef __terark_set_op_h__
#define __terark_set_op_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

// #include <boost/preprocessor/iteration/local.hpp>
// #include <boost/preprocessor/enum.hpp>
// #include <boost/preprocessor/enum_params.hpp>
// #include <boost/mpl/at.hpp>
// #include <boost/mpl/map.hpp>
//

#include <assert.h>
#include <stdlib.h>
#include <vector>
#include <algorithm>
#include <functional>
#include <stdexcept>
#include <boost/tuple/tuple.hpp>
#include <boost/type_traits.hpp>
#include <boost/mpl/if.hpp>
#include <boost/multi_index/identity.hpp>
#include "stdtypes.hpp"
#include "util/compare.hpp"

namespace terark {
//@{
/**
 @brief multi set intersection -- result is copy from [_First1, _Last1)

 [dest, result) is [_First1, _Last1) AND [_First2, _Last2)

 predictions:
   for any [x1, x2) in [_First1, _Last1), _Pred(x2, x1) = false,
   that is say: x1 <= x2
   allow same key in sequence
 @note
  - result is copy from [_First1, _Last1)
  - values in sequence can be equal, when values are equal, multi values will be copied
  - _InIt1::value_type, _InIt2::value_type, _OutIt::value_type need not be same
  - _InIt1::value_type and _InIt2::value_type must comparable by _Pr
  - _InIt1::value_type must assignable to _OutIt::value_type
  - _InIt2::value_type need not assignable to _OutIt::value_type
 */
template<class _InIt1, class _InIt2, class _OutIt, class _Pr>
inline
_OutIt multiset_intersection(_InIt1 _First1, _InIt1 _Last1,
							 _InIt2 _First2, _InIt2 _Last2, _OutIt dest, _Pr _Pred)
{
	for (; _First1 != _Last1 && _First2 != _Last2; )
	{
		if (_Pred(*_First1, *_First2))
			++_First1;
		else if (_Pred(*_First2, *_First1))
			++_First2;
		else
			*dest++ = *_First1++; // do not increment _First2
	}
	return (dest);
}

template<class _InIt1, class _RandInIt2, class _OutIt, class _Pr>
inline
_OutIt multiset_1small_intersection(_InIt1 _First1, _InIt1 _Last1,
									_RandInIt2 _First2, _RandInIt2 _Last2, _OutIt dest, _Pr _Pred)
{
	for (; _First1 != _Last1 && _First2 != _Last2; )
	{
		std::pair<_RandInIt2, _RandInIt2> range = std::equal_range(_First2, _Last2, *_First1, _Pred);
		if (range.first == range.second)
			++_First1;
		else {
			// !(*range.first < *_First1) --> *_First1 <= *range.first
			while (_First1 != _Last1 && !_Pred(*range.first, *_First1))
				*dest++ = *_First1++;
		}
		_First2 = range.second;
	}
	return (dest);
}

template<class _InIt1, class _RandInIt2, class _OutIt, class _Pr>
inline
_OutIt multiset_fast_intersection(_InIt1 _First1, _InIt1 _Last1,
								  _RandInIt2 _First2, _RandInIt2 _Last2, _OutIt dest, _Pr _Pred, ptrdiff_t threshold = 32)
{
	if (std::distance(_First1, _Last1) * threshold < std::distance(_First2, _Last2))
		return multiset_1small_intersection(_First1, _Last1, _First2, _Last2, dest, _Pred);
	else
		return multiset_intersection(_First1, _Last1, _First2, _Last2, dest, _Pred);
}
//@}

//////////////////////////////////////////////////////////////////////////

//@{
/**
 @brief multi set intersection2 -- result is copy from [_First2, _Last2)

 [dest, result) is [_First1, _Last1) AND [_First2, _Last2)
 for any [x1, x2) in [_First1, _Last1), _Pred(x2, x1) = false
 allow same key in sequence

 @note
  - result is copy from [_First2, _Last2)
  - values in sequence can be equal, when values are equal, multi values will be copied
  - _InIt1::value_type, _InIt2::value_type, _OutIt::value_type need not be same
  - _InIt1::value_type and _InIt2::value_type must comparable by _Pr
  - _InIt2::value_type must assignable to _OutIt::value_type
  - _InIt1::value_type need not assignable to _OutIt::value_type
 */
template<class _InIt1, class _InIt2, class _OutIt, class _Pr>
inline
_OutIt multiset_intersection2(_InIt1 _First1, _InIt1 _Last1,
							  _InIt2 _First2, _InIt2 _Last2, _OutIt dest, _Pr _Pred)
{
	for (; _First1 != _Last1 && _First2 != _Last2; )
	{
		if (_Pred(*_First1, *_First2))
			++_First1;
		else if (_Pred(*_First2, *_First1))
			++_First2;
		else
			*dest++ = *_First2++; // do not increment _First1
	}
	return (dest);
}

template<class _InIt1, class _RandInIt2, class _OutIt, class _Pr>
inline
_OutIt multiset_1small_intersection2(_InIt1 _First1, _InIt1 _Last1,
									 _RandInIt2 _First2, _RandInIt2 _Last2, _OutIt dest, _Pr _Pred)
{
	for (; _First1 != _Last1 && _First2 != _Last2; )
	{
		std::pair<_RandInIt2, _RandInIt2> range = std::equal_range(_First2, _Last2, *_First1, _Pred);
		if (range.first != range.second)
		{
			dest = std::copy(range.first, range.second, dest);
		}
		_First2 = range.second;
		++_First1;
	}
	return (dest);
}

template<class _RandInIt1, class _RandInIt2, class _OutIt, class _Pr>
inline
_OutIt multiset_fast_intersection2(_RandInIt1 _First1, _RandInIt1 _Last1,
								   _RandInIt2 _First2, _RandInIt2 _Last2, _OutIt dest, _Pr _Pred, ptrdiff_t threshold = 32)
{
	if (std::distance(_First1, _Last1) * threshold < std::distance(_First2, _Last2))
		return multiset_1small_intersection2(_First1, _Last1, _First2, _Last2, dest, _Pred);
	else
		return multiset_intersection2(_First1, _Last1, _First2, _Last2, dest, _Pred);
}
//@}

//! 一定是调用 pred(prev, curr), 而不是 pred(curr, prev)
//!
//! 可以保证对有序序列 pred(prev, curr) 等效于 !equal_to(prev, curr)
//! 从而可以对有序序列使用 set_unique(first, last, terark::not2(less_than_comp))
//! 来删除重复元素
//!
template<class _FwdIt, class _Pr> inline
_FwdIt set_unique(_FwdIt _First, _FwdIt _Last, _Pr _Pred)
{	// remove each matching previous
	for (_FwdIt _Firstb; (_Firstb = _First) != _Last && ++_First != _Last; )
		if (_Pred(*_Firstb, *_First))
			{	// copy down
			for (; ++_First != _Last; )
				if (!_Pred(*_Firstb, *_First))
					*++_Firstb = *_First;
			return (++_Firstb);
			}
	return (_Last);
}

template<class _FwdIt> inline
	_FwdIt set_unique(_FwdIt _First, _FwdIt _Last)
{
	return set_unique(_First, _Last,
		std::equal_to<typename std::iterator_traits<_FwdIt>::value_type>()
		);
}

template<class _Pr>
class not1_functor
{
	_Pr m_pr;

public:
	typedef bool result_type;

	explicit not1_functor(_Pr _Pred) : m_pr(_Pred) {}
	not1_functor() {}

	template<class T1>
	bool operator()(const T1& x) const
	{
		return !m_pr(x);
	}
};

template<class _Pr>
class not2_functor
{
	_Pr m_pr;

public:
	typedef bool result_type;

	explicit not2_functor(_Pr _Pred) : m_pr(_Pred) {}
	not2_functor() {}

	template<class T1, class T2>
	bool operator()(const T1& x, const T2& y) const
	{
		return !m_pr(x, y);
	}
};

template<class _Pr>
not1_functor<_Pr> not1(const _Pr& _Pred)
{
	return not1_functor<_Pr>(_Pred);
}
template<class _Pr>
not2_functor<_Pr> not2(const _Pr& _Pred)
{
	return not2_functor<_Pr>(_Pred);
}

//////////////////////////////////////////////////////////////////////////
//! find in predicted near range, if not found, find it in the whole range
//!
template<class IterT, class CompareT>
IterT find_next_larger(IterT first, IterT prediction, IterT last, CompareT comp)
{
	IterT iter = std::upper_bound(first, prediction, *first, comp);
	if (iter == prediction && !comp(*first, *prediction)) // *first == *prediction
		return std::upper_bound(prediction, last, *first, comp);
	else
		return iter;
}

//////////////////////////////////////////////////////////////////////////

//! Call Convert(*iter) to convert the iterator
template<class InputIter, class ConvertedType, class Convertor>
class convertor_iterator_adaptor :
	public std::iterator< typename std::iterator_traits<InputIter>::iterator_category
						, ConvertedType>
{
	typedef convertor_iterator_adaptor<InputIter, ConvertedType, Convertor> self_t;

	Convertor m_convert;

public:
	InputIter m_iter;

	typedef typename std::iterator_traits<InputIter>::difference_type difference_type;

	explicit convertor_iterator_adaptor(InputIter iter, const Convertor& convert = Convertor())
		: m_iter(iter), m_convert(convert) {}

	ConvertedType operator*() const { return m_convert(*m_iter); }

	self_t& operator++() { ++m_iter; return *this; }
	self_t& operator--() { --m_iter; return *this; }

	self_t operator++(int) { self_t temp(*this); ++m_iter; return temp; }
	self_t operator--(int) { self_t temp(*this); --m_iter; return temp; }

	bool operator==(const self_t& y) const { return m_iter == y.m_iter; }
	bool operator!=(const self_t& y) const { return m_iter != y.m_iter; }

	bool operator<=(const self_t& y) const { return m_iter <= y.m_iter; }
	bool operator>=(const self_t& y) const { return m_iter >= y.m_iter; }

	bool operator<(const self_t& y) const { return m_iter < y.m_iter; }
	bool operator>(const self_t& y) const { return m_iter > y.m_iter; }

	difference_type operator-(const self_t& y) const { return m_iter - y.m_iter; }

	self_t  operator+(difference_type y) const { return self_t(m_iter + y, m_convert); }

	self_t& operator+=(difference_type i) { m_iter += i; return *this; }
	self_t& operator-=(difference_type i) { m_iter -= i; return *this; }
};
template<class ConvertedType, class InputIter, class Convertor>
convertor_iterator_adaptor<InputIter, ConvertedType, Convertor>
convert_iterator(InputIter iter, const Convertor& convertor)
{
	return convertor_iterator_adaptor<InputIter, ConvertedType, Convertor>(iter, convertor);
}

//! Call extractor(iter) to convert source iter to target iter
//!
//! if *iter is something computed from iter, the adapter can avoid to calling *iter
//! @note
//!  -# Extractor is applied on iter, not '*iter'
//!  -# Extractor should be a functor
template<class InputIter, class MemberType, class Extractor>
class member_iterator_adaptor
  : public std::iterator< typename std::iterator_traits<InputIter>::iterator_category
						, MemberType>
{
	typedef member_iterator_adaptor<InputIter, MemberType, Extractor> self_t;

private:
	Extractor m_extr;

public:
	InputIter m_iter;

	typedef typename std::iterator_traits<InputIter>::difference_type difference_type;

	explicit member_iterator_adaptor(InputIter iter, const Extractor& convert = Extractor())
		: m_iter(iter), m_extr(convert) {}

	MemberType operator*() const { return m_extr(m_iter); }

	self_t& operator++() { ++m_iter; return *this; }
	self_t& operator--() { --m_iter; return *this; }

	self_t operator++(int) { self_t temp(*this); ++m_iter; return temp; }
	self_t operator--(int) { self_t temp(*this); --m_iter; return temp; }

	bool operator==(const self_t& y) const { return m_iter == y.m_iter; }
	bool operator!=(const self_t& y) const { return m_iter != y.m_iter; }

	bool operator<=(const self_t& y) const { return m_iter <= y.m_iter; }
	bool operator>=(const self_t& y) const { return m_iter >= y.m_iter; }

	bool operator<(const self_t& y) const { return m_iter < y.m_iter; }
	bool operator>(const self_t& y) const { return m_iter > y.m_iter; }

	difference_type operator-(const self_t& y) const { return m_iter - y.m_iter; }

	self_t  operator+(difference_type y) const { return self_t(m_iter + y, m_extr); }

	self_t& operator+=(difference_type i) { m_iter += i; return *this; }
	self_t& operator-=(difference_type i) { m_iter -= i; return *this; }
};
template<class MemberType, class InputIter, class Extractor>
member_iterator_adaptor<InputIter, MemberType, Extractor>
member_iterator(InputIter iter, const Extractor& convertor)
{
	return member_iterator_adaptor<InputIter, MemberType, Extractor>(iter, convertor);
}
//////////////////////////////////////////////////////////////////////////

template<class Iter>
typename std::iterator_traits<Iter>::value_type
sum(Iter first, Iter last)
{
	typename std::iterator_traits<Iter>::value_type s =
	typename std::iterator_traits<Iter>::value_type();
	for (; first != last; ++first)
		s += *first;
	return s;
}
template<class C>
typename C::value_type
sum(const C& c)
{
	return sum(c.begin(), c.end());
}
template<class Iter, class Extractor>
typename Extractor::key_type
sum(Iter first, Iter last, Extractor ex)
{
	typename Extractor::key_type s = typename Extractor::key_type();
	for (; first != last; ++first)
		s += ex(*first);
	return s;
}
template<class C, class Extractor>
typename Extractor::key_type
sum(const C& c, Extractor ex)
{
	return sum(c.begin(), c.end(), ex);
}

//////////////////////////////////////////////////////////////////////////
/**
 @brief
 - like std::remove_if, but use std::swap
 */
template<class ForwIter, class Pred>
ForwIter
remove_swap_if(ForwIter first, ForwIter last, Pred pred) {
	for (; first != last; ++first)
		if (pred(*first))
			goto DoErase;
	return last;
DoErase: {
	ForwIter dest = first;
	for (++first; first != last; ++first) {
		using std::swap;
		if (!pred(*first))
			swap(*dest, *first), ++dest;
	}
	return dest;
  }
}

namespace multi_way
{

/**
 @brief can be better inline when pass by param
 */
class FirstEqualSecond {
public:
	template<class T>
	bool operator()(const std::pair<T,T>& x)const{return x.first==x.second;}
};

class MustProvideKeyExtractor;

struct tag_cache_default {}; //!< default select
struct tag_cache_none {};
struct tag_cache_key {};
struct tag_cache_value {};

template<class Value>
class CacheValueArray {
protected: // sizeof(*this) == sizeof(void*)
	Value* m_cache;
public:
	CacheValueArray() { m_cache = NULL; }
	~CacheValueArray() {
		if (m_cache)
			delete []m_cache;
	}
	void resize_cache(size_t size) {
		if (m_cache)
			delete []m_cache;
		m_cache = new Value[size];
		if (NULL == m_cache)
			abort(); // non-exception-safe, must abort
	}
};

template<class Value>
class CacheValueArray0 {
protected: // sizeof(*this) >= 3 * sizeof(void*)
	std::vector<Value> m_cache;
public:
	void resize_cache(size_t size) { m_cache.resize(size); }
};

template<class Key, class KeyExtractor>
class KeyExtractor_get_key : protected KeyExtractor {
public:
	template<class Value>
	const Key& get_key(const Value& x) const { return (*this)(x); }
};

template<class InputIter, class KeyType, class KeyExtractor, class HowCache>
class CacheStrategy; // template proto-type definition

template<class InputIter, class KeyType, class KeyExtractor>
class CacheStrategy<InputIter, KeyType, KeyExtractor, tag_cache_none>
  : protected KeyExtractor_get_key<KeyType, KeyExtractor>
{
public:
	typedef typename std::iterator_traits<InputIter>::value_type value_type;
	typedef tag_cache_none           cache_category;
	typedef boost::tuples::null_type cache_item_type;
protected:
	//! 这个函数必须要带 cache_category 参数
	//! 对 tag_cache_none, 这个函数是在 derived class 中实现的
	//! 但是，因为 derived class 中使用了 using super::get_cache_key, 所以这里必须有 get_cache_key 成员
	//! Just an empty function
	inline void get_cache_key(size_t/*,tag_cache_none*/) const {}
	inline void load_cache_item(size_t, const InputIter&) {/*do nothing...*/}
	inline void resize_cache(size_t) {/*do nothing...*/}

// just used by LoserTree::start_yan_wu
// has no this function, LoserTree::start_yan_wu don't support tag_cache_none
// 	void set_cache_item(size_t i, const cache_item_type&) {}
};

template<class InputIter, class KeyType, class KeyExtractor>
class CacheStrategy<InputIter, KeyType, KeyExtractor, tag_cache_key>
  : protected CacheValueArray<KeyType>
  , protected KeyExtractor_get_key<KeyType, KeyExtractor>
{
public:
	typedef typename std::iterator_traits<InputIter>::value_type value_type;
	typedef tag_cache_key           cache_category;
	typedef KeyType                 cache_item_type;
protected:
	//! 这个函数必须要带 cache_category 参数
	inline const KeyType&
	get_cache_key(size_t nth,tag_cache_key)const{return this->m_cache[nth];}

	inline void
	load_cache_item(size_t i,InputIter p){this->m_cache[i]=this->get_key(*p);}

	// just used by LoserTree::start_yan_wu
	void set_cache_item(size_t i, const KeyType& k) { this->m_cache[i] = k; }
};

template<class InputIter, class KeyType, class KeyExtractor>
class CacheStrategy<InputIter, KeyType, KeyExtractor, tag_cache_value>
  : protected CacheValueArray<typename std::iterator_traits<InputIter>::value_type>
  , protected KeyExtractor_get_key<KeyType, KeyExtractor>
{
public:
	typedef typename std::iterator_traits<InputIter>::value_type value_type;
	typedef tag_cache_value           cache_category;
	typedef value_type                cache_item_type;
protected:
	//! 这个函数必须要带 cache_category 参数
	inline const KeyType&
	get_cache_key(size_t i, tag_cache_value) const {
		return this->get_key(this->m_cache[i]); }

	inline void
	load_cache_item(size_t i, InputIter iter) { this->m_cache[i] = *iter; }

	// just used by LoserTree::start_yan_wu
	void set_cache_item(size_t i, const value_type& x){this->m_cache[i] = x;}
};

template<class InputIter, class KeyType, class KeyExtractor>
class CacheStrategy<InputIter, KeyType, KeyExtractor, tag_cache_default>
	/**
	 @brief cache_item_type is KeyType or value_type

	 为了 cache 的效率，仅缓存必要的数据(CPU cache, or disk cache)
	 - input_iterator_tag 表示同一个数据只从 iterator 读一次，因此缓存整个 value
	 - forward_iterator_tag 及更高级，可以允许从 iterator 读多次，因此只缓存 key
	 */
	: public boost::mpl::if_c<
		boost::is_same<std::input_iterator_tag,
					   typename std::iterator_traits<InputIter>::iterator_category
					  >::value,
		CacheStrategy<InputIter, KeyType, KeyExtractor, tag_cache_value>,
		CacheStrategy<InputIter, KeyType, KeyExtractor, tag_cache_key>
	>::type
{};

template< class way_iter_t
		, class KeyType = typename std::iterator_traits<way_iter_t>::value_type
		, bool StableSort = false //!< same Key in various way will output by way order
		, class Compare = std::less<KeyType>
		, class KeyExtractor = typename boost::mpl::if_c<
				boost::is_same<KeyType,
							   typename std::iterator_traits<way_iter_t>::value_type
							  >::value,
				boost::multi_index::identity<KeyType>,
				MustProvideKeyExtractor
			>::type
	    , class HowCache = tag_cache_default
		>
class LoserTree : protected Compare,
	public CacheStrategy<way_iter_t, KeyType, KeyExtractor, HowCache>
{
	DECLARE_NONE_COPYABLE_CLASS(LoserTree)
	typedef CacheStrategy<way_iter_t, KeyType, KeyExtractor, HowCache> super;
public:
	typedef typename std::iterator_traits<way_iter_t>::value_type value_type;
	typedef KeyType  key_type;
	typedef KeyExtractor key_extractor;
	typedef boost::integral_constant<bool, StableSort> is_stable_sort;
	typedef typename super::cache_category  cache_category;
	typedef typename super::cache_item_type cache_item_type;
	typedef boost::mpl::true_  TriCompare1, StableSort1;
	typedef boost::mpl::false_ TriCompare0, StableSort0;

public:
	/**
	 @brief construct

	 @par 图示如下：
	 @code

     m_ways                                             this is guard value
      ||                                                          ||
	  ||                                                          ||
	  \/                                                          \/
	 -------> 0 way_iter_t [min_value.........................max_value]
	   /      1 way_iter_t [min_value.........................max_value] <--- 每个序列均已
	   |      2 way_iter_t [min_value.........................max_value]      按 comp 排序
	   |      3 way_iter_t [min_value.........................max_value]
	  <       4 way_iter_t [min_value.........................max_value]
	   |      5 way_iter_t [min_value.........................max_value]
	   |      7 way_iter_t [min_value.........................max_value]
	   \      8 way_iter_t [min_value.........................max_value]
	 -------> end

	 @endcode

	 @param comp    value 的比较器

	 @note 每个序列最后必须要有一个 max_value 作为序列结束标志，否则会导致未定义行为
	 */
	LoserTree(const KeyType& max_key,
			  const Compare& comp = Compare(),
			  const KeyExtractor& keyExtractor = KeyExtractor())
		: Compare(comp), m_max_key(max_key)
	{
		m_tree = NULL;
		static_cast<KeyExtractor&>(*this) = keyExtractor;
	}
	~LoserTree() {
		if (m_tree) free(m_tree);
	}

	/**
	 @brief 初始化

	- 共有 n 个内部结点，n 个外部结点
	- winner 只用于初始化时计算败者树，算完后即丢弃
	- winner/loser 的第 0 个单元都不是内部结点，不属于树中的一员
	- winner 的第 0 个单元未用
	- m_tree 的第 0 个单元用于保存最终的赢者, 其它单元保存败者

	- 该初始化需要的 n-1 次比较，总的时间复杂度是 O(n)

	- 严蔚敏&吴伟民 的 LoserTree 初始化复杂度是 O(n*log(n))，并且还需要一个 min_key,
	  但是他们的初始化不需要额外的 winner 数组

    - 并且，这个实现比 严蔚敏&吴伟民 的 LoserTree 初始化更强壮
	 */
	void start() {
		size_t len = m_ways.size();
		if (terark_unlikely(len <= 0))
			throw std::invalid_argument("LoserTree: way sequence must not be empty");

		{
			size_t* t = (size_t*)realloc(m_tree, sizeof(size_t) * len);
			if (NULL == t) throw std::bad_alloc();
			m_tree = t;
		}
		this->resize_cache(len);
		size_t i;
		for (i = 0; i != len; ++i) {
			// load first value from every sequence
			this->load_cache_item(i, m_ways[i]);
		}
		if (terark_unlikely(1 == len)) {
			m_head = 0;
			return;
		}

		size_t minInnerToEx = len / 2;
		std::vector<size_t> winner(len);

		for (i = len - 1; i > minInnerToEx; --i)
			exter_loser_winner(m_tree[i], winner[i], i, len);
		size_t left, right;
		if (len & 1) { // odd
		// left child is last inner node, right child is first external node
			left = winner[len-1];
			right = 0;
		} else {
			left = 0;
			right = 1;
		}
		get_loser_winner(m_tree[minInnerToEx], winner[minInnerToEx], left, right);

		for (i = minInnerToEx; i > 0; i /= 2)
			for (size_t j = i; j > i/2; ) {
				--j;
				inner_loser_winner(m_tree[j], winner[j], j, winner);
			}
		m_head = winner[1];
	}

	//! 严蔚敏&吴伟民 的 LoserTree 初始化
	//! 复杂度是 O(n*log(n))，并且还需要一个 min_key
	void start_yan_wu(const cache_item_type& min_item,
					  const cache_item_type& max_item)
	{
		//! this function do not support tag_cache_none
		size_t len = m_ways.size();
		this->resize_cache(len+1);
		this->set_cache_item(len, min_item);

		size_t i;
		for (i = 0; i < len; ++i) {
			m_tree[i] = len;
			// load first value from every sequence
			this->load_cache_item(i, m_ways[i]);
		}
		for (i = len; i > 0; ) ajust(--i);

		// 防止 cache 的最后一个成员上升到 top ??.....
		//
		this->set_cache_item(len, max_item);

	//	assert(!m_ways.empty());
	//	if (m_head == len)
	//		ajust(len); // 会导致在 ajust 中 m_tree[parent] 越界
	}

	const value_type& current_value() const {
		assert(!m_ways.empty());
	//	assert(!empty()); // 允许访问末尾的 guardValue, 便于简化 app
		return current_value_aux(cache_category());
	}

	/**
	 @brief return current way NO.
	 */
	size_t current_way() const {
		assert(!m_ways.empty());
	//	assert(!empty()); // allow this use
		return m_head;
	}

	size_t total_ways() const { return m_ways.size(); }
	bool is_any_way_end() const { return empty(); }

	bool empty() const {
		assert(!m_ways.empty());
		const KeyType& cur_key = get_cache_key(m_head, cache_category());
		const Compare& cmp = *this;
		return !cmp(cur_key, m_max_key); // cur_key >= max_value
	}

	void increment() {
		assert(!m_ways.empty());
		assert(!empty());
		size_t top = m_head;
		this->load_cache_item(top, ++m_ways[top]);
		ajust(top);
	}

	void ajust_for_update_top() {
		assert(!m_ways.empty());
		size_t top = m_head;
		this->load_cache_item(top, m_ways[top]);
		ajust(top);
	}

	way_iter_t& top_iter() { assert(!m_ways.empty()); return  m_ways[m_head]; }
	const size_t* get_tree() const { return m_tree; }

protected:
	void ajust(size_t s) {
		size_t parent = s + m_ways.size();
		size_t*ptree =  m_tree;
		while ((parent /= 2) > 0) {
			size_t& tparent = ptree[parent];
			if (comp_cache_item(tparent, s, cache_category(), is_stable_sort()))
				std::swap(s, tparent);
		}
		m_head = s;
	}

	void exter_loser_winner(size_t& loser, size_t& winner, size_t parent, size_t len) const {
		size_t left  = 2 * parent - len;
		size_t right = left + 1;
		get_loser_winner(loser, winner, left, right);
	}
	void inner_loser_winner(size_t& loser, size_t& winner, size_t parent,
				const std::vector<size_t>& winner_vec) const {
		size_t left  = 2 * parent;
		size_t right = 2 * parent + 1;
		left  = winner_vec[left];
		right = winner_vec[right];
		get_loser_winner(loser, winner, left, right);
	}
	void get_loser_winner(size_t& loser, size_t& winner, size_t left, size_t right) const {
		if (comp_cache_item(left, right, cache_category(), is_stable_sort())) {
			loser  = right;
			winner = left;
		} else {
			loser = left;
			winner = right;
		}
	}

	const value_type& current_value_aux(tag_cache_none) const {
		assert(m_head < m_ways.size());
		return *m_ways[m_head];
	}
	const value_type& current_value_aux(tag_cache_key) const {
		assert(m_head < m_ways.size());
		return *m_ways[m_head];
	}
	const value_type& current_value_aux(tag_cache_value) const {
		assert(m_head < m_ways.size());
		return this->m_cache[m_head];
	}

	using super::get_cache_key;
	inline const KeyType& get_cache_key(size_t nth, tag_cache_none) const {
		return this->get_key(*m_ways[nth]); }

	template<class CacheCategory>
	inline bool
	comp_cache_item(size_t x, size_t y, CacheCategory, StableSort0) const {
		const KeyType& kx = get_cache_key(x, CacheCategory());
		const KeyType& ky = get_cache_key(y, CacheCategory());
		return static_cast<const Compare&>(*this)(kx, ky);
	}
	template<class CacheCategory>
	inline bool
	comp_cache_item(size_t x, size_t y, CacheCategory, StableSort1) const {
		const KeyType& kx = get_cache_key(x, CacheCategory());
		const KeyType& ky = get_cache_key(y, CacheCategory());
		return comp_key_stable(x, y, kx, ky, HasTriCompare<Compare>());
	}
	inline bool
	comp_key_stable(size_t x, size_t y, const KeyType& kx, const KeyType& ky, TriCompare1) const {
		ptrdiff_t ret = Compare::compare(kx, ky);
		if (ret < 0) return true;
		if (ret > 0) return false;
		ret = Compare::compare(kx, m_max_key);
		assert(ret <= 0);
		if (0==ret) return false;
		else        return x < y;
	}
	inline bool
	comp_key_stable(size_t x, size_t y, const KeyType& kx, const KeyType& ky, TriCompare0) const {
		const Compare& cmp = *this;
		if ( cmp(kx, ky)) return true;
		if ( cmp(ky, kx)) return false;
		if (!cmp(kx, m_max_key)) {// kx >= max_key --> kx == max_key
			// max_key is the max, so must assert this:
			assert(!cmp(m_max_key, kx));
			return false;
		}
		else return x < y;
	}

protected:
	size_t* m_tree;
	size_t  m_head; // access m_tree[0] need an extra memory load
	KeyType m_max_key;
public:
	std::vector<way_iter_t> m_ways;
typedef LoserTree MyType;
#include "multi_way_basic.hpp"
#include "multi_way_algo_loser_tree.hpp"
};

template<class _InIt>
struct Heap_WayIterEx : public std::pair<_InIt, _InIt> {
	size_t index;
	template<class _InIt2>
	Heap_WayIterEx(const Heap_WayIterEx<_InIt2>& y)
	  : std::pair<_InIt, _InIt>(y.first, y.second), index(y.index) {}
	Heap_WayIterEx(size_t index, const _InIt& i1, const _InIt& i2)
	  : std::pair<_InIt, _InIt>(i1, i2), index(index) {}
};
template<class _InIt>
Heap_WayIterEx<_InIt>
make_way_iter_ex(size_t index, const _InIt& i1, const _InIt& i2) {
	return Heap_WayIterEx<_InIt>(index, i1, i2); }

template< class _InIt
		, class KeyType = typename std::iterator_traits<_InIt>::value_type
		, bool StableSort = false //!< same Key in various way will output by way order
		, class Compare = std::less<KeyType>
		, class KeyExtractor = typename boost::mpl::if_c<
				boost::is_same<KeyType,
							   typename std::iterator_traits<_InIt>::value_type
							  >::value,
				boost::multi_index::identity<KeyType>,
				MustProvideKeyExtractor
			>::type
		, class WayIter = typename boost::mpl::if_c<StableSort, Heap_WayIterEx<_InIt>, std::pair<_InIt, _InIt> >::type
		>
class HeapMultiWay_TrivalWayIter : private Compare {
	DECLARE_NONE_COPYABLE_CLASS(HeapMultiWay_TrivalWayIter)
public:
	typedef typename std::iterator_traits<_InIt>::value_type value_type;
	typedef KeyExtractor key_extractor;
	typedef KeyType  key_type;
	typedef Compare  compare_t;
	typedef WayIter  way_iter_t;
	typedef boost::mpl::true_  TriCompare1, StableSort1;
	typedef boost::mpl::false_ TriCompare0, StableSort0;

	class HeapCompare {
		const HeapMultiWay_TrivalWayIter& heap;
	public:
		//! 反转输入参数，用做堆操作的比较
		bool operator()(const way_iter_t& x, const way_iter_t& y) const {
			assert(x.first != x.second);
			assert(y.first != y.second);
			boost::integral_constant<bool, StableSort> is_stable_sort;
			return heap.comp_item_aux(y, x, is_stable_sort);
		}
		explicit HeapCompare(const HeapMultiWay_TrivalWayIter& heap) : heap(heap) {}
	};
	friend class HeapCompare;

public:
	HeapMultiWay_TrivalWayIter(Compare comp = Compare()) : Compare(comp) {}

	void start() {
	#ifndef NDEBUG
		for (size_t i = 0, n = m_ways.size(); i < n; ++i) {
			assert(m_ways[i].first != m_ways[i].second); }
	#endif
		m_old_size = m_ways.size();
		std::make_heap(m_ways.begin(), m_ways.end(), HeapCompare(*this));
	}

	bool empty() { return m_ways.empty(); }

	const value_type& current_value() const {
		assert(!m_ways.empty());
		return *m_ways.front().first;
	}

	const WayIter& current_input() const {
		assert(!m_ways.empty());
		return  m_ways.front();
	}

	size_t current_way() const {
		assert(!m_ways.empty());
		return  m_ways.front().index;
	}

	void increment() {
		assert(!m_ways.empty());
		++m_ways.front().first;
		ajust_for_update_top();
	}

	void ajust_for_update_top() {
		if (FirstEqualSecond()(m_ways.front())) {
			pop_heap_ignore_top(m_ways.begin(), m_ways.end(), HeapCompare(*this));
			m_ways.pop_back();
		} else
			adjust_heap_top(m_ways.begin(), m_ways.end(), HeapCompare(*this));
	}

	WayIter& top_iter() { assert(!m_ways.empty()); return m_ways.front(); }

	/*
	 @par 图示如下：
	 @code
	   [                m_ways                   ]
	 0 [first..............................second]
	 1 [first..............................second]
	 2 [first..............................second] <--- 每个序列均已按 comp 排序
	 3 [first..............................second]
	 4 [first..............................second]
	 5 [first..............................second]
	 @endcode
	 way_iter_t 是一个 pair<_InIt, _InIt>，表示一个递增序列 [first, second)
	*/
	std::vector<way_iter_t> m_ways; // public
private:
	size_t     m_old_size;

	inline bool
	comp_item_aux(const way_iter_t& x, const way_iter_t& y, StableSort0) const {
		assert(x.first != x.second);
		assert(y.first != y.second);
		return (*this)(*x.first, *y.first);
	}
	inline bool
	comp_item_aux(const way_iter_t& x, const way_iter_t& y, StableSort1) const {
		assert(x.first != x.second);
		assert(y.first != y.second);
		return comp_key_stable(x.index, y.index, *x.first, *y.first,
				typename HasTriCompare<Compare>::type());
	}
	inline bool
	comp_key_stable(size_t x, size_t y, const value_type& kx, const value_type& ky, TriCompare1) const {
		ptrdiff_t ret = Compare::compare(kx, ky);
		if (ret < 0) return true;
		if (ret > 0) return false;
		else         return x < y;
	}
	inline bool
	comp_key_stable(size_t x, size_t y, const value_type& kx, const value_type& ky, TriCompare0) const {
		const Compare& cmp = *this;
		if (cmp(kx, ky)) return true;
		if (cmp(ky, kx)) return false;
		else             return x < y;
	}
public:
	size_t total_ways() const { return m_old_size; }
	bool is_any_way_end() const { return m_ways.size() != m_old_size; }

typedef HeapMultiWay_TrivalWayIter MyType;
#include "multi_way_basic.hpp"
#include "multi_way_algo_heap.hpp"
};

template< class InputIter
		, class KeyType = typename std::iterator_traits<InputIter>::value_type
		, bool StableSort = false //!< same Key in various way will output by way order
		, class Compare = std::less<KeyType>
		, class KeyExtractor = typename boost::mpl::if_c<
				boost::is_same<KeyType,
					typename std::iterator_traits<InputIter>::value_type
							  >::value,
				boost::multi_index::identity<KeyType>,
				MustProvideKeyExtractor
			>::type
	    , class HowCache = tag_cache_default
		>
class HeapMultiWay : private Compare,
	public CacheStrategy<InputIter, KeyType, KeyExtractor, HowCache>
{
	DECLARE_NONE_COPYABLE_CLASS(HeapMultiWay)
	typedef CacheStrategy<InputIter, KeyType, KeyExtractor, HowCache> super;
public:
	typedef std::pair<InputIter, InputIter>  iipair;
	typedef typename std::iterator_traits<InputIter>::value_type value_type;
	typedef KeyType      key_type;
	typedef KeyExtractor key_extractor;
	typedef typename super::cache_category cache_category;
	typedef boost::mpl::true_  TriCompare1, StableSort1;
	typedef boost::mpl::false_ TriCompare0, StableSort0;

	class HeapCompare {
		const HeapMultiWay& heap;
	public:
		//! 反转输入参数，用做堆操作的比较
		inline bool operator()(size_t x, size_t y) const {
			assert(heap.m_ways[x].first != heap.m_ways[x].second);
			assert(heap.m_ways[y].first != heap.m_ways[y].second);
			boost::integral_constant<bool, StableSort> is_stable_sort;
			return heap.comp_cache_item(y, x, cache_category(), is_stable_sort);
		}
		explicit HeapCompare(const HeapMultiWay& heap) : heap(heap) {}
	};
	friend class HeapCompare;

public:
	/**
	 @brief construct

	 @par 图示如下：
	 @code
	 m_ways     first                              second
      ||         ||                                   ||
	  ||         ||                                   ||
	  \/         \/                                   \/
	  0 iipair [min_value..............................)
	  1 iipair [min_value..............................) <--- 每个序列均已
	  2 iipair [min_value..............................)      按 comp 排序
	  3 iipair [min_value..............................)
	  4 iipair [min_value..............................)
	  5 iipair [min_value..............................)
	  7 iipair [min_value..............................)
	  8 iipair [min_value..............................)
	 @endcode

	 @param first, last [in,out] 每个元素都是一个 iipair, 表示一个递增子序列 [first, second)
	 @param comp    value 的比较器

	 @note
	  - 一般情况下，这个类的性能不如 LoserTree, 但是，这个类也有一些优点：
	    - 对每个序列用 make_pair(first, last) 来表示，概念上更清晰
	    - 序列最后不需要一个 max_value 结束标志，这一点上胜于 LoserTree
		  - 当无法提供 max_value 时，使用这个类也许是更好的办法，比如当 int key 在整个 int 值域内都有定义时
	  - LoserTree 的每个序列只用一个 iterator 表示，序列结束用 MAX_KEY 做标志
	 */
	HeapMultiWay(const Compare& comp = Compare(),
				 const KeyExtractor& keyExtractor = KeyExtractor())
	{
		static_cast<KeyExtractor&>(*this) = keyExtractor;
	}
	void start() {
		assert(!m_ways.empty());
		m_old_size = m_ways.size();
		m_heap.reserve(m_old_size);
		this->resize_cache(m_old_size);
		for (size_t way_idx = 0; way_idx < m_old_size; ++way_idx) {
			if (m_ways[way_idx].first != m_ways[way_idx].second) {
				m_heap.push_back(way_idx);
				// read first value from every sequence
				this->load_cache_item(way_idx, m_ways[way_idx].first);
			}
		}
		std::make_heap(m_heap.begin(), m_heap.end(), HeapCompare(*this));
	}

	bool empty() const { return m_heap.empty(); }

	const value_type& current_value() const {
		assert(!m_heap.empty());
		return current_value_aux(cache_category());
	}
	size_t current_way() const { assert(!m_heap.empty()); return m_heap[0]; }

	void increment() {
		assert(!m_heap.empty());
		size_t top_way = m_heap[0];
		iipair& top = *(m_ways + top_way);
		++top.first;
		if (terark_unlikely(top.first == top.second)) {
			pop_heap_ignore_top(m_heap.begin(), m_heap.end(), HeapCompare(*this));
			m_heap.pop_back();
		} else {
			this->load_cache_item(top_way, top.first);
			adjust_heap_top(m_heap.begin(), m_heap.end(), HeapCompare(*this));
		}
	}

	void ajust_for_update_top() {
		assert(!m_heap.empty());
		size_t top_way = m_heap[0];
		iipair& top = m_ways[top_way];
	//	++top.first; // caller may inc multi for top.first
		if (terark_unlikely(top.first == top.second)) {
			pop_heap_ignore_top(m_heap.begin(), m_heap.end(), HeapCompare(*this));
			m_heap.pop_back();
		} else {
			this->load_cache_item(top_way, top.first);
			adjust_heap_top(m_heap.begin(), m_heap.end(), HeapCompare(*this));
		}
	}

	iipair& top_iter() { assert(!m_heap.empty()); return m_ways[m_heap[0]]; }

protected:
	const value_type& current_value_aux(tag_cache_none) const {
		assert(m_heap[0] < m_old_size);
		return *(*(m_ways + m_heap[0])).first;
	}
	const value_type& current_value_aux(tag_cache_key) const {
		assert(m_heap[0] < m_old_size);
		return *(*(m_ways + m_heap[0])).first;
	}
	const value_type& current_value_aux(tag_cache_value) const {
		assert(m_heap[0] < m_old_size);
		return this->m_cache[m_heap[0]];
	}

	using super::get_cache_key;
	inline KeyType get_cache_key(size_t nth, tag_cache_none) const {
		assert(nth < m_old_size);
		const iipair& p = *(m_ways + nth);
		assert(p.first != p.second);
		return this->get_key(*p.first);
	}

	template<class CacheCategory>
	inline bool
	comp_cache_item(size_t x, size_t y,	CacheCategory, StableSort0) const {
		const KeyType& kx = get_cache_key(x, CacheCategory());
		const KeyType& ky = get_cache_key(y, CacheCategory());
		return static_cast<const Compare&>(*this)(kx, ky);
	}
	template<class CacheCategory>
	inline bool
	comp_cache_item(size_t x, size_t y, CacheCategory, StableSort1) const {
		const KeyType& kx = get_cache_key(x, CacheCategory());
		const KeyType& ky = get_cache_key(y, CacheCategory());
		return comp_key_stable(x, y, kx, ky, HasTriCompare<Compare>());
	}
	inline bool
	comp_key_stable(size_t x, size_t y, const KeyType& kx, const KeyType& ky, TriCompare1) const {
		ptrdiff_t ret = Compare::compare(kx, ky);
		if (ret < 0) return true;
		if (ret > 0) return false;
		else         return x < y;
	}
	inline bool
	comp_key_stable(size_t x, size_t y, const KeyType& kx, const KeyType& ky, TriCompare0) const {
		const Compare& cmp = *this;
		if (cmp(kx, ky)) return true;
		if (cmp(ky, kx)) return false;
		else             return x < y;
	}

	bool is_any_way_end() const { return m_heap.size() != m_old_size; }
	size_t total_ways() const { return m_old_size; }

protected:
	std::vector<iipair> m_ways;
	std::vector<size_t> m_heap;
	size_t			m_old_size;

typedef HeapMultiWay MyType;
#include "multi_way_basic.hpp"
#include "multi_way_algo_heap.hpp"
};


//! TODO:

template<class InputIterator>
class InputIteratorReader {
	InputIterator m_end;
public:
	bool empty(const InputIterator& iter) const { return iter != m_end; }
};

/**
 @brief 当相同元素的数目超过 minDup 时，将元素拷贝到输出

 这是一个很有用的 multi_way_copy_if._Cond, 并且也作为一个编写 _Cond 的示例
 可以在 MultiWay_CopyAtLeastDup 中对 value 进行某方面的统计（例如记录每个 value 的重复次数）
 */
class MultiWay_CopyAtLeastDup {
	ptrdiff_t m_minDup;
public:
	MultiWay_CopyAtLeastDup(ptrdiff_t minDup) : m_minDup(minDup) {}

	template<class ValueT>
	bool operator()(ptrdiff_t equal_count,
					const ValueT& /*prev_value*/,
					const ValueT& /*curr_value*/) const
	{
		// 仅当 equal_count 第一次达到 m_minDup 时才返回 true
		// 返回 equal_count >= m_minDup 会导致多次拷贝

		return equal_count == m_minDup;
	}
};

} // namespace multi_way

template<class Container>
class any_inserter_iterator :
	public std::iterator<std::output_iterator_tag, typename Container::value_type>
{
public:
	typedef Container container_type;
	typedef typename Container::reference reference;

	explicit any_inserter_iterator(Container& _Cont) : container(&_Cont) {}

	template<class T>
	any_inserter_iterator&
	operator=(const T& x) { container->insert(x); return *this; }

	// pretend to return designated value
	any_inserter_iterator& operator*() { return *this; }

	// pretend to preincrement
	any_inserter_iterator& operator++() { return *this; }

	// pretend to postincrement
	any_inserter_iterator operator++(int) { return *this; }

protected:
	Container *container;
};

template<class Container>
any_inserter_iterator<Container>
any_inserter(Container& c) { return any_inserter_iterator<Container>(c); }

} // namespace terark

#endif // __terark_set_op_h__

// @} end file set_op.hpp


