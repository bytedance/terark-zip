/* vim: set tabstop=4 : */
/********************************************************************
	@file heap_ext.hpp
	@brief seperated from set_op.hpp by Lei Peng at 2017-07-25

	@date	2006-10-23 14:02
	@author	leipeng
	@{
*********************************************************************/
# pragma once

#include <assert.h>

namespace terark {

// move hole upward, and put val on the final hole
template<class RandIter, class Int, class T, class Compare>
void
terark_heap_hole_up(RandIter first, Int hole, Int top, T val, Compare comp) {
	Int parent;
	while (hole > top && comp(first[parent = (hole-1)/2], val)) {
		first[hole] = first[parent];
		hole = parent;
	}
	first[hole] = val;
}

template<class RandIter, class Int, class Compare>
Int
terark_heap_hole_down(RandIter first, Int hole, Int len, Compare comp) {
	Int child2; // second child
	while ((child2 = 2*hole+2) < len) {
		if (comp(first[child2], first[child2-1]))
			child2--;
		first[hole] = first[child2];
		hole = child2;
	}
	if (child2 == len) {
		child2--;
		first[hole] = first[child2];
		hole = child2;
	}
	return hole;
}

template<class RandIter, class Int, class T, class Compare>
void
terark_adjust_heap(RandIter first, Int hole, Int len, T val, Compare comp) {
	Int leaf_hole = terark_heap_hole_down(first, hole, len, comp);
	Int top = hole;
	terark_heap_hole_up(first, leaf_hole, top, val, comp);
}

template<class RandIter, class Int, class Compare>
inline void
pop_heap_ignore_top(RandIter first, Int len, Compare comp) {
	if (len-- > 1)
		terark_adjust_heap(first, Int(0), len, *(first+len), comp);
}
template<class RandIter, class Compare>
inline void
pop_heap_ignore_top(RandIter first, RandIter last, Compare comp) {
	pop_heap_ignore_top(first, last-first, comp);
}

template<class RandIter, class Int, class Compare>
inline void
adjust_heap_top(RandIter first, Int len, Compare comp) {
	if (len > 1)
		terark_adjust_heap(first, Int(0), len, *first, comp);
}
template<class RandIter, class Compare>
inline void
adjust_heap_top(RandIter first, RandIter last, Compare comp) {
	adjust_heap_top(first, last-first, comp);
}

//---------------------------------------------------------------------------
//
// move hole upward, and put val on the final hole
template<class RandIter, class Int, class T, class Compare, class SyncIndex>
void
terark_heap_hole_up(RandIter first, Int hole, Int top, T val, Compare comp,
					SyncIndex sync) {
	Int parent;
	while (hole > top && comp(first[parent = (hole-1)/2], val)) {
		first[hole] = first[parent];
		sync(first[hole], hole);
		hole = parent;
	}
	first[hole] = val;
	sync(first[hole], hole);
}

template<class RandIter, class Int, class Compare, class SyncIndex>
void
terark_heap_hole_down(RandIter first, Int hole, Int len, Compare comp,
					  SyncIndex sync) {
	Int child2; // second child
	while ((child2 = 2*hole+2) < len) {
		if (comp(first[child2], first[child2-1]))
			child2--;
		first[hole] = first[child2];
		sync(first[hole], hole);
		hole = child2;
	}
	if (child2 == len) {
		child2--;
		first[hole] = first[child2];
		sync(first[hole], hole);
		hole = child2;
	}
	return hole;
}

// sync first[hole] in heap `[first, first + len)` with new value `val`
// and ajust the heap
template<class RandIter, class Int, class T, class Compare, class SyncIndex>
void
terark_adjust_heap(RandIter first, Int hole, Int len, T val, Compare comp,
				   SyncIndex sync) {
	Int leaf_hole = terark_heap_hole_down(first, hole, len, comp, sync);
	Int top = hole;
	terark_heap_hole_up(first, leaf_hole, top, val, comp, sync);
}

// first[hole] has updated, adjust the heap
template<class RandIter, class Int, class Compare, class SyncIndex>
inline void
adjust_heap_hole(RandIter first,Int hole,Int len,Compare comp,SyncIndex sync){
	terark_adjust_heap(first, hole, len, first[hole], comp, sync);
}

// first[0] is hole, and will be lost, not same with std::pop_heap
// std::pop_heap will move first[0] to last[-1], pop_heap_ignore_top will not
// after this function call, last[-1] will become gabage, you should remove it
// @code
//		vector<T*> vec;
//		...
//		pop_heap_ignore_top(vec.begin(), vec.end(), comp, SyncIndex());
//		vec.pop_back(); // remove last[-1]
// @endcode
template<class RandIter, class Int, class Compare, class SyncIndex>
inline void
pop_heap_ignore_top(RandIter first, Int len, Compare comp, SyncIndex sync) {
	if (len-- > 1)
		terark_adjust_heap(first, Int(0), len, *(first+len), comp, sync);
}
template<class RandIter, class Compare, class SyncIndex>
inline void
pop_heap_ignore_top(RandIter first,RandIter last,Compare comp,SyncIndex sync){
	pop_heap_ignore_top(first, last-first, comp, sync);
}

// first[0] has updated, adjust the heap to cordinate new value of first[0]
template<class RandIter, class Int, class Compare, class SyncIndex>
inline void
adjust_heap_top(RandIter first, Int len, Compare comp, SyncIndex sync) {
	if (len > 1)
		terark_adjust_heap(first, Int(0), len, *first, comp, sync);
}
template<class RandIter, class Compare, class SyncIndex>
inline void
adjust_heap_top(RandIter first, RandIter last, Compare comp, SyncIndex sync) {
	adjust_heap_top(first, last-first, comp, sync);
}

} // namespace terark

// @} end file heap_ext.hpp


