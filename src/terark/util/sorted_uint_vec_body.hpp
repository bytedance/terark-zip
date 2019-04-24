size_t blockIdx = idx >> m_log2_blockUnits;
size_t blockUnits = size_t(1) << m_log2_blockUnits;
size_t indexWidth = m_offsetWidth + m_sampleWidth;
size_t indexBitPos = indexWidth * blockIdx;
auto   indexBase = (const size_t*)(m_index);
size_t offset0 = febitvec::s_get_uint(indexBase, indexBitPos, m_offsetWidth); indexBitPos += m_offsetWidth;
size_t sample0 = febitvec::s_get_uint(indexBase, indexBitPos, m_sampleWidth);
#if defined(NDEBUG)
#define sample1 febitvec::s_get_uint(indexBase, indexBitPos + m_offsetWidth + m_sampleWidth, m_sampleWidth)
#else
indexBitPos += m_sampleWidth;
size_t offset1 = febitvec::s_get_uint(indexBase, indexBitPos, m_offsetWidth); indexBitPos += m_offsetWidth;
size_t sample1 = febitvec::s_get_uint(indexBase, indexBitPos, m_sampleWidth);
#endif
size_t subIdx = idx & ~(size_t(-1) << m_log2_blockUnits);

// pLargeBase is always aligned at size_t, faster for getting large units.
// 'pData' may not aligned at size_t,
// 'pData' is used for read multiple small units by size_t
auto   pLargeBase = (size_t const*)(m_data.data() + sizeof(ObjectHeader));

auto   header = m_data.data() + sizeof(ObjectHeader) + offset0;
assert(offset0 + 2 <= offset1);
assert(sample0 <= sample1);

switch (GetDDWidthType(header)) {
case 0: { // bits = 0, all diffdiff are zero
	assert(offset0 + 2 <= offset1);
	size_t headerLen;
	size_t loWater = GetLoWater_x(header, &headerLen);
	aVal[0] = sample0 + loWater * subIdx;
	if (subIdx + 1 < blockUnits) {
		aVal[1] = aVal[0] + loWater;
	} else {
		aVal[1] = sample1;
	}
	break; }
case 1:
#define Width 1
	#include "sorted_uint_vec_case.hpp"
case 2:
#define Width 2
	#include "sorted_uint_vec_case.hpp"
case 3:
#define Width 3
	#include "sorted_uint_vec_case.hpp"
case 4:
#define Width 4
	#include "sorted_uint_vec_case.hpp"
case 5:
#define Width 5
	#include "sorted_uint_vec_case.hpp"
case 6:
#define Width 6
	#include "sorted_uint_vec_case.hpp"
case 7:
#define Width 7
	#include "sorted_uint_vec_case.hpp"
case 8:
#define Width 8
	#include "sorted_uint_vec_case.hpp"
case 9:
#define Width 9
	#include "sorted_uint_vec_case.hpp"
case 10:
#define Width 10
	#include "sorted_uint_vec_case.hpp"
case 11:
{
	// smallWidth and largeWidth are both 1, this is an optimization
	size_t headerLen;
	size_t const loWater = GetLoWater_x(header, &headerLen);
	size_t const* pData = (const size_t*)(uintptr_t(header) + headerLen);
	size_t i = 0, ddsum = 0;
	for (; i < subIdx / 64; ++i) {
		ddsum += fast_popcount64(pData[i]);
	}
	uint64_t w = pData[i];
	ddsum += fast_popcount_trail(w, subIdx % 64);
	aVal[0] = sample0 + ddsum + loWater * subIdx;
	if (subIdx + 1 < blockUnits)
		aVal[1] = aVal[0] + loWater + ((w >> subIdx % 64) & 1);
	else
		aVal[1] = sample1;
	break;
}
case 12:
#define Width 12
	#include "sorted_uint_vec_case.hpp"
case 13:
#define Width 16
	#include "sorted_uint_vec_case.hpp"
case 14:
#define Width 20
	#include "sorted_uint_vec_case.hpp"
case 15:
{
	// smallWidth and largeWidth are both 2, this is an optimization
	size_t headerLen;
	size_t const loWater = GetLoWater_x(header, &headerLen);
	size_t const* pData = (const size_t*)(uintptr_t(header) + headerLen);
	size_t i = 0, ddsum = 0;
	for (; i < subIdx / 32; ++i) {
		uint64_t w = pData[i];
		w = (w & 0x3333333333333333ull) + ((w >> 2) & 0x3333333333333333ull);
		w = (w & 0x0F0F0F0F0F0F0F0Full) + ((w >> 4) & 0x0F0F0F0F0F0F0F0Full);
		w = (w * 0x0101010101010101ull) >> 56;
		ddsum += w;
	}
	uint64_t w = pData[i];
	size_t shiftBits = (subIdx % 32)*2;
	if (terark_likely(subIdx % 32)) {
		uint64_t x = w & ~(size_t(-1) << shiftBits);
		x = (x & 0x3333333333333333ull) + ((x >> 2) & 0x3333333333333333ull);
		x = (x & 0x0F0F0F0F0F0F0F0Full) + ((x >> 4) & 0x0F0F0F0F0F0F0F0Full);
		x = (x * 0x0101010101010101ull) >> 56;
		ddsum += x;
	}
	aVal[0] = sample0 + ddsum + loWater * subIdx;
	if (subIdx + 1 < blockUnits)
		aVal[1] = aVal[0] + loWater + ((w >> shiftBits) & 3);
	else
		aVal[1] = sample1;
	break;
}

} // switch
