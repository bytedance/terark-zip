#include "sorted_uint_vec.hpp"
#include <terark/bitmap.hpp>
#include <terark/num_to_str.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/util/function.hpp>
#include <boost/intrusive_ptr.hpp>

namespace terark {

static inline size_t safe_bsr_u64(uint64_t val) {
    return val ? terark_bsr_u64(val) : 0;
}

static inline uint64_t encode_signed(int64_t x) {
	if (x < 0)
		return ((-x-1) << 1) | 1;
	else
		return (+x << 1);
}

static inline int64_t decode_signed(uint64_t x) {
	if (x & 1)
		return -int64_t(x >> 1) - 1;
	else
		return +int64_t(x >> 1);
}

template<class Uint>
static inline int64_t
varbits_get_signed(const Uint* base, size_t bitpos, size_t width) {
	Uint val = febitvec::s_get_uint(base, bitpos, width);
	return decode_signed(val);
}

/*
template<class Uint>
static inline void
varbits_set_signed(Uint* base, size_t bitpos, size_t width, int64_t sval) {
	BOOST_STATIC_ASSERT(sizeof(Uint) >= 8);
	Uint val = encode_signed(sval);
	febitvec::s_set_uint(base, bitpos, width, val);
}
*/

// Lagrange polynomial interpolation:
static inline
int64_t lagrange(int64_t x, int64_t a, int64_t f_a
                          , int64_t m, int64_t f_m
                          , int64_t b, int64_t f_b
                          , size_t log2_range) {
    assert(0 == a);
    assert(m == b/2);
    assert(b == int64_t(1) << log2_range);
    assert(x != 0);
    assert(x != m);
    typedef long double RealNum;
#if 0
    int64_t f_x = int64_t(0
          + f_a*RealNum((x-m)*(x-b))/((a-m)*(a-b))
          + f_m*RealNum((x-a)*(x-b))/((m-a)*(m-b))
          + f_b*RealNum((x-a)*(x-m))/((b-a)*(b-m))
        );
#else
    int64_t one = 1;
//  int64_t denominator;
    int64_t deno_a = +(one << (log2_range*2 - 1));
//  int64_t deno_m = -(one << (log2_range*2 - 2));
    int64_t deno_m = -(deno_a >> 1);
//  int64_t deno_b = +(one << (log2_range*2 - 1));
    int64_t deno_b = deno_a;
    int64_t f_x = int64_t(0
          + f_a*RealNum((x-m)*(x-b))/deno_a
          + f_m*RealNum((x-a)*(x-b))/deno_m
          + f_b*RealNum((x-a)*(x-m))/deno_b
        );
#endif
    return f_x;
};

static inline uint32_t GetDDWidthType(const byte_t* ba) {
	return ba[0] & 0x0F;
}

// for other diffdiffWidthType
static inline uint64_t GetLoWater_x(const byte_t* ba, size_t* headerLen) {
	byte_t type = (ba[0] >> 4) & 3;
	uint64_t val = ba[0] >> 6; // get low 2 bits
	switch (type) {
	case 0: // total 10 bits, max 1023
		*headerLen = 2;
		val |= uint64_t(ba[1]) << 2;
		break;
	case 1: // total 18 bits, max 256K - 1
		*headerLen = 3;
		val |= uint64_t(unaligned_load<uint16_t>(ba+1)) << 2;
		break;
	case 2: // total 26 bits, max  64M -1
		*headerLen = 4;
		val |= uint64_t(unaligned_load<uint32_t>(ba+1) & 0x00FFFFFF) << 2;
		break;
	case 3: // total 50 bits, max   1P -1
		*headerLen = 7;
		val |= uint64_t(unaligned_load<uint64_t>(ba+1) & ~(uint64_t(-1LL)<<48)) << 2;
		break;
	}
	return val;
}

static const byte_t g_used_small_width[65] = {0,1,2,3,4,5,6,7,8,9,10
	,  12 // width = 11, type = NA
	,  12 // width = 12, type = 12 +++
	,  16 // width = 13, type = NA
	,  16 // width = 14, type = NA
	,  16 // width = 15, type = NA
	,  16 // width = 16, type = 13 +++
	,  20 // width = 17, type = NA
	,  20 // width = 18, type = NA
	,  20 // width = 19, type = NA
	,  20 // width = 20, type = 14 +++
	      // 21 numbers above --------------
	,  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	,  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	,  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

static size_t getWidthType(size_t smallWidth, size_t largeWidth) {
	// type  width
	// 11 ->  1 bit, no large units
	// 13 -> 16
	// 14 -> 20
	// 15 ->  2 bits, no large units
	static const byte_t widthToType[21] = {0,1,2,3,4,5,6,7,8,9,10
		, 255 // width = 11, type = NA
		,  12 // width = 12, type = 12 +++
		, 255 // width = 13, type = NA
		, 255 // width = 14, type = NA
		, 255 // width = 15, type = NA
		,  13 // width = 16, type = 13 +++
		, 255 // width = 17, type = NA
		, 255 // width = 18, type = NA
		, 255 // width = 19, type = NA
		,  14 // width = 20, type = 14 +++
	};
	assert(smallWidth <= 20);
	assert(smallWidth <= largeWidth);
	uint32_t widthType;
	if (1 == smallWidth && 1 == largeWidth) {
		widthType = 11;
	}
	else if (2 == smallWidth && 2 == largeWidth) {
		widthType = 15;
	}
	else {
		widthType = widthToType[smallWidth];
		assert(widthType < 16);
	}
	return widthType;
}

static inline
void WriteHeader(valvec<byte_t>* data, uint64_t loWater,
				 size_t smallWidth, size_t largeWidth) {
	const size_t widthType = getWidthType(smallWidth, largeWidth);

	// +-------------+-------------+---------------------------+
	// |  loWaterLo  |  loWaterHi  |        widthType          |
	// |    2 bits   | Length Type |   Small Unit Width Type   |
	// +-------------+-------------+---------------------------+
	// |   7  |   6  |   5  |   4  |   3  |   2  |   1  |   0  |
	// +-------------+-------------+---------------------------+

	const uint08_t loWaterLo = uint08_t(loWater &  3) << 6;
	const uint64_t loWaterHi = uint64_t(loWater >> 2);
	const uint64_t one = 1;

// LoWaterType use 2 bits, it map to how many bytes of 'loWaterHi' requires
#define WriteHeaderData(LoWaterType, HiBytes) \
	static_assert(LoWaterType >= 0 && LoWaterType < 4, "Bad LoWaterType"); \
	data->append(uint08_t(loWaterLo | (LoWaterType << 4) | widthType)); \
	data->append((const byte*)(&loWaterHi), HiBytes)

	if (false) {}
	else if (loWaterHi < one <<  8) { WriteHeaderData(0, 1); }
	else if (loWaterHi < one << 16) { WriteHeaderData(1, 2); }
	else if (loWaterHi < one << 24) { WriteHeaderData(2, 3); }
	else if (loWaterHi < one << 48) { WriteHeaderData(3, 6); }
	else {
		abort(); // should not happen
	}
}

static const uint64_t
// 21*3 = 63, high 1 bit into overflowEx, seperate next high 3 bits
Zc01_21_03 = 0b0001001001001001001001001001001001001001001001001001001001001001,
Bits_20_03 = 0b0000000111000111000111000111000111000111000111000111000111000111,
Bits_10_06 = 0b0000000000111111000000111111000000111111000000111111000000111111,
Bits_05_01 = 0b0000000000000001000000000001000000000001000000000001000000000001,

Zc03_10_06 = 0b0000000111000111000111000111000111000111000111000111000111000111,
Zc01_10_06 = 0b0000000001000001000001000001000001000001000001000001000001000001,

Zc02_12_05 = 0b0000000110001100011000110001100011000110001100011000110001100011,
Zc01_12_05 = 0b0000000010000100001000010000100001000010000100001000010000100001,
Bits_12_05 = 0b0000000001111100000111110000011111000001111100000111110000011111,
Bits_06_01 = 0b0000000000000100000000010000000001000000000100000000010000000001,

// Zc03 -- 3 bits of 1 in one 7 bits group
Zc03_09_07 = 0b0000011100001110000111000011100001110000111000011100001110000111,
Zc01_09_07 = 0b0000000100000010000001000000100000010000001000000100000010000001,
Bits_08_07 = 0b0000000000000001111111000000011111110000000111111100000001111111,
Bits_04_14 = 0b0000000000000000000000111111111111110000000000000011111111111111,

// 7*9 = 63, high 1 bit into overflowEx, seperate next high 7 bits
Zc03_07_09 = 0b0000000111000000111000000111000000111000000111000000111000000111,
Zc01_07_09 = 0b0000000001000000001000000001000000001000000001000000001000000001,
Bits_06_09 = 0b0000000000000000000111111111000000000111111111000000000111111111,
Bits_03_18 = 0b0000000000000000000000000001000000000000000001000000000000000001,

Bits_06_10 = 0b0000000000000011111111110000000000111111111100000000001111111111;

static inline size_t s_get_mask(size_t bits) {
	if (sizeof(size_t)*8 == bits)
		return size_t(-1);
	else
		return ~(size_t(-1) << bits);
}

#define WordUnits      (64/Width)
#define UnitMask       ( (size_t(+1) <<    Width) - 1 )
#define HiExMaskToLow  ( (size_t(+1) << 64%Width) - 1 )
#define HiExMaskAtHig   ~(size_t(-1) >> 64%Width)

struct SortedUintVec::ObjectHeader {
	uint64_t  units : 48; // SortedUintVec::m_size
	uint64_t  log2_blockUnits : 4; // 6 or 7
	uint64_t  offsetWidth_1   : 6; // 1 ~ 64, = real_offset_width - 1
	uint64_t  sampleWidth_1   : 6; // 1 ~ 64, = real_sample_width - 1
	uint64_t  indexOffset     :48;
	uint64_t  is_overall_full_sorted: 1;
	uint64_t  is_samples_full_sorted: 1;
	uint64_t  reserved : 14;
};

void SortedUintVec::get2(size_t idx, size_t aVal[2]) const {
    static_assert(sizeof(ObjectHeader)==16, "sizeof(ObjectHeader) must be 16");
    assert(m_is_sorted_uint_vec);
    assert(idx < m_size);
    size_t blockIdx = idx >> m_log2_blockUnits;
    size_t blockUnits = size_t(1) << m_log2_blockUnits;
    size_t indexWidth = m_offsetWidth + m_sampleWidth;
    size_t indexBitPos = indexWidth * blockIdx;
    auto   indexBase = (const size_t*)(m_index);
    size_t offset0 = febitvec::s_get_uint(indexBase, indexBitPos, m_offsetWidth); indexBitPos += m_offsetWidth;
    size_t sample0 = febitvec::s_get_uint(indexBase, indexBitPos, m_sampleWidth);
    indexBitPos += m_sampleWidth;
    size_t offset1 = febitvec::s_get_uint(indexBase, indexBitPos, m_offsetWidth); indexBitPos += m_offsetWidth;
    size_t sample1 = febitvec::s_get_uint(indexBase, indexBitPos, m_sampleWidth);
    size_t subIdx = idx & (blockUnits-1);

    // pLargeBase is always aligned at size_t, faster for getting large units.
    // 'pData' may not aligned at size_t,
    // 'pData' is used for read multiple small units by size_t
    auto   pLargeBase = (size_t const*)(m_data.data() + sizeof(ObjectHeader));

    auto   header = m_data.data() + sizeof(ObjectHeader) + offset0;
    assert(offset0 + 2 <= offset1);
    assert(sample0 <= sample1 || !m_is_samples_full_sorted);

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
    case 11: {
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
        break; }
    case 12:
#define Width 12
#include "sorted_uint_vec_case.hpp"
    case 13:
#define Width 16
#include "sorted_uint_vec_case.hpp"
    case 14:
#define Width 20
#include "sorted_uint_vec_case.hpp"
    case 15: {
        // smallWidth and largeWidth are both 2, this is an optimization
        size_t headerLen;
        size_t const loWater = GetLoWater_x(header, &headerLen);
        byte_t const* pData = (const byte_t*)(uintptr_t(header) + headerLen);
        size_t const old_style_block_size = headerLen + blockUnits * 2 / 8;
        if (offset1 - offset0 > old_style_block_size) {
            size_t fixWidth = (pData[0] & 63) + 1;
            size_t baseBitPos = (headerLen + offset0) * 8 + 6;
            size_t a = 0;
            size_t m = blockUnits/2;
            size_t b = blockUnits;
            size_t log2_range = m_log2_blockUnits;
            int64_t headerVal = loWater;
            int64_t f_a = 0;
            int64_t f_b = sample1 - sample0;
            int64_t f_m = (f_a+f_b)/2 + decode_signed(headerVal);
            auto computeVal = [=](size_t x) {
                assert(x > 0);
                uint64_t val;
                if (terark_likely(blockUnits/2 != x)) {
                    size_t x_idx = (x < blockUnits/2) ? (x-1) : (x-2);
                    size_t bitpos = baseBitPos + fixWidth*x_idx;
                    int64_t diff = varbits_get_signed(pLargeBase, bitpos, fixWidth);
                    int64_t f_x_lag = lagrange(x, a, f_a, m, f_m, b, f_b, log2_range);
                    val = f_x_lag + diff;
                //  printf("read: x = %3zd , f_x_lag = %3zd , diff = %3zd\n", x, f_x_lag, diff);
                } else {
                    val = f_m;
                }
                return sample0 + val;
            };
            if (terark_likely(subIdx)) {
                aVal[0] = computeVal(subIdx);
            } else {
                aVal[0] = sample0;
            }
            if (terark_likely(subIdx + 1 < blockUnits)) {
                aVal[1] = computeVal(subIdx + 1);
            } else {
                aVal[1] = sample1;
            }
            break; // break the case
        }
        size_t i = 0, ddsum = 0;
        for (; i < subIdx / 32; ++i) {
            uint64_t w = unaligned_load<uint64_t>(pData, i);
            w = (w & 0x3333333333333333ull) + ((w >> 2) & 0x3333333333333333ull);
            w = (w & 0x0F0F0F0F0F0F0F0Full) + ((w >> 4) & 0x0F0F0F0F0F0F0F0Full);
            w = (w * 0x0101010101010101ull) >> 56;
            ddsum += w;
        }
        uint64_t w = unaligned_load<uint64_t>(pData, i);
        size_t shiftBits = (subIdx % 32) * 2;
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
        break; }
    } // switch
}

/// aVal capacity must be at least blockUnits(64 or 128)
void SortedUintVec::get_block(size_t blockIdx, size_t* aVals) const {
    assert(blockIdx << m_log2_blockUnits < m_size + (size_t(1) << m_log2_blockUnits) - 1);
    assert(m_is_sorted_uint_vec);
    size_t blockUnits = size_t(1) << m_log2_blockUnits;
    size_t indexWidth = m_offsetWidth + m_sampleWidth;
    size_t indexBitPos = indexWidth * blockIdx;
    auto   indexBase = (const size_t*)(m_index);
    size_t offset0 = febitvec::s_get_uint(indexBase, indexBitPos, m_offsetWidth); indexBitPos += m_offsetWidth;
    size_t sample0 = febitvec::s_get_uint(indexBase, indexBitPos, m_sampleWidth);
    indexBitPos += m_sampleWidth;
    size_t offset1 = febitvec::s_get_uint(indexBase, indexBitPos, m_offsetWidth); indexBitPos += m_offsetWidth;
    size_t sample1 = febitvec::s_get_uint(indexBase, indexBitPos, m_sampleWidth);
    // pLargeBase is always aligned at size_t, faster for getting large units.
    // 'pData' may not aligned at size_t,
    // 'pData' is used for read multiple small units by size_t
    auto   pLargeBase = (size_t const*)(m_data.data() + sizeof(ObjectHeader));

    auto   header = m_data.data() + sizeof(ObjectHeader) + offset0;
    assert(offset0 + 2 <= offset1);
    assert(sample0 <= sample1 || !m_is_samples_full_sorted);

    switch (GetDDWidthType(header)) {
    case 0: { // bits = 0, all diffdiff are zero
        assert(offset0 + 2 <= offset1);
        size_t headerLen;
        size_t loWater = GetLoWater_x(header, &headerLen);
        size_t val = sample0;
        for (size_t i = 0; i < blockUnits; ++i) {
            aVals[i] = val;
            val += loWater;
        }
        break; }
    case 1:
#define Width 1
#include "sorted_uint_vec_get_block_case.hpp"
    case 2:
#define Width 2
#include "sorted_uint_vec_get_block_case.hpp"
    case 3:
#define Width 3
#include "sorted_uint_vec_get_block_case.hpp"
    case 4:
#define Width 4
#include "sorted_uint_vec_get_block_case.hpp"
    case 5:
#define Width 5
#include "sorted_uint_vec_get_block_case.hpp"
    case 6:
#define Width 6
#include "sorted_uint_vec_get_block_case.hpp"
    case 7:
#define Width 7
#include "sorted_uint_vec_get_block_case.hpp"
    case 8:
#define Width 8
#include "sorted_uint_vec_get_block_case.hpp"
    case 9:
#define Width 9
#include "sorted_uint_vec_get_block_case.hpp"
    case 10:
#define Width 10
#include "sorted_uint_vec_get_block_case.hpp"
    case 11: {
        // smallWidth and largeWidth are both 1, this is an optimization
        size_t headerLen;
        size_t const loWater = GetLoWater_x(header, &headerLen);
        size_t const* pData = (const size_t*)(uintptr_t(header) + headerLen);
        size_t val = sample0;
        for(size_t i = 0; i < blockUnits / TERARK_WORD_BITS; ++i) {
            auto   w = unaligned_load<size_t>(&pData[i]);
            for (size_t j = 0; j < TERARK_WORD_BITS; ++j) {
                aVals[i*TERARK_WORD_BITS + j] = val;
                val += loWater + (w & 1);
                w >>= 1;
            }
        }
        break; }
    case 12:
#define Width 12
#include "sorted_uint_vec_get_block_case.hpp"
    case 13:
#define Width 16
#include "sorted_uint_vec_get_block_case.hpp"
    case 14:
#define Width 20
#include "sorted_uint_vec_get_block_case.hpp"
    case 15: {
        // smallWidth and largeWidth are both 2, this is an optimization
        size_t headerLen;
        size_t const loWater = GetLoWater_x(header, &headerLen);
        byte_t const* pData = (const byte_t*)(uintptr_t(header) + headerLen);
        size_t const old_style_block_size = headerLen + blockUnits * 2 / 8;
        if (offset1 - offset0 > old_style_block_size) {
            size_t log2_range = m_log2_blockUnits;
            size_t headerVal = loWater;
            int64_t f_a = 0;
            int64_t f_b = sample1 - sample0;
            int64_t f_m = (f_a+f_b)/2 + decode_signed(headerVal);
            size_t plainWidth = (pData[0] & 63) + 1;
            assert(plainWidth <= 64);
            size_t bitpos = (headerLen + offset0) * 8 + 6;
            size_t a = 0;
            size_t b = blockUnits;
            size_t m = (b - a) / 2;
            aVals[0] = sample0;
            size_t i = 1;
            for (; i < blockUnits/2; ++i) {
                int64_t diff = varbits_get_signed(pLargeBase, bitpos, plainWidth);
                int64_t f_x_lag = lagrange(i, a, f_a, m, f_m, b, f_b, log2_range);
                aVals[i] = sample0 + f_x_lag + diff;
                bitpos += plainWidth;
            }
            aVals[i++] = sample0 + f_m; // do not inc bitpos
            for (; i < blockUnits; ++i) {
                int64_t diff = varbits_get_signed(pLargeBase, bitpos, plainWidth);
                int64_t f_x_lag = lagrange(i, a, f_a, m, f_m, b, f_b, log2_range);
                aVals[i] = sample0 + f_x_lag + diff;
                bitpos += plainWidth;
            }
            break; // break the case
        }
        size_t val = sample0;
        for(size_t i = 0; i < blockUnits / (TERARK_WORD_BITS/2); ++i) {
            auto   w = unaligned_load<size_t>(pData, i);
            for (size_t j = 0; j < TERARK_WORD_BITS/2; ++j) {
                aVals[i*(TERARK_WORD_BITS/2) + j] = val;
                val += loWater + (w & 3);
                w >>= 2;
            }
        }
        break; }
    } // switch
}

size_t SortedUintVec::get(size_t idx) const {
	assert(idx < m_size);
	assert(m_is_sorted_uint_vec);
	size_t aVal[2];
	get2(idx, aVal);
	return aVal[0];
}

SortedUintVec::SortedUintVec() {
	m_index = NULL;
	m_log2_blockUnits = 0;
	m_offsetWidth = 0;
	m_sampleWidth = 0;
	m_is_sorted_uint_vec = true;
	m_is_overall_full_sorted = true;
	m_is_samples_full_sorted = true;
#if TERARK_WORD_BITS == 64
	m_padding = 0xCCCCCCCC;
#endif
	m_size = 0;
}

SortedUintVec::~SortedUintVec() {
}

size_t SortedUintVec::lower_bound(size_t lo, size_t hi, size_t key) const {
    assert(lo <= hi);
    if (terark_unlikely(lo == hi)) {
        return lo;
    }
    size_t blockUnits = block_units(), mask = blockUnits - 1;
    size_t hiBlk = (hi + mask) >> m_log2_blockUnits;
    size_t loBlk = lo >> m_log2_blockUnits;
    size_t i = loBlk;
    if (loBlk + 1 < hiBlk) {
        auto indexBase = (const size_t* )(m_index);
        auto sampleWidth = m_sampleWidth;
        auto indexWidth = m_offsetWidth + sampleWidth;
        if (!m_is_samples_full_sorted) {
            size_t nextSample = s_get_block_min_val(
                indexBase, sampleWidth, indexWidth, loBlk+1);
            if (nextSample < key) {
                i = ++loBlk;
                lo = loBlk << m_log2_blockUnits;
            }
            else
                goto SearchBlock;
        }
        size_t j = hiBlk;
        while (i < j) {
            size_t mid_idx = (i + j) / 2;
            size_t mid_pos = indexWidth * (mid_idx + 1) - sampleWidth;
            size_t mid_val = febitvec::s_get_uint(indexBase, mid_pos, sampleWidth);
            if (mid_val < key)
                i = mid_idx + 1;
            else
                j = mid_idx;
        }
        if (terark_unlikely(loBlk == i)) {
            size_t hit_pos = indexWidth * (i + 1) - sampleWidth;
            size_t hit_val = febitvec::s_get_uint(indexBase, hit_pos, sampleWidth);
            if (key < hit_val) {
                return lo;
            }
        }
        else {
            i--;
        }
    }
SearchBlock:
    assert(blockUnits <= 128);
    size_t block[128]; get_block(i, block);
    size_t l = (i+0 == loBlk) ? lo & mask : 0;
    size_t u = (i+1 == hiBlk) ? ((hi-1) & mask) + 1 : blockUnits;
    size_t k = lower_bound_n(block, l, u, key);
    size_t p = k + (i << m_log2_blockUnits);
    return p;
}

size_t SortedUintVec::upper_bound(size_t lo, size_t hi, size_t key) const {
    assert(lo <= hi);
    if (terark_unlikely(lo == hi)) {
        return lo;
    }
    size_t blockUnits = block_units(), mask = blockUnits - 1;
    size_t hiBlk = (hi + mask) >> m_log2_blockUnits;
    size_t loBlk = lo >> m_log2_blockUnits;
    size_t i = loBlk;
    if (loBlk + 1 < hiBlk) {
        auto indexBase = (const size_t*)(m_index);
        auto sampleWidth = m_sampleWidth;
        auto indexWidth = m_offsetWidth + sampleWidth;
        if (!m_is_samples_full_sorted) {
            size_t nextSample = s_get_block_min_val(
                indexBase, sampleWidth, indexWidth, loBlk+1);
            if (nextSample <= key) {
                i = ++loBlk;
                lo = loBlk << m_log2_blockUnits;
            }
            else
                goto SearchBlock;
        }
        size_t j = hiBlk;
        while (i < j) {
            size_t mid_idx = (i + j) / 2;
            size_t mid_pos = indexWidth * (mid_idx + 1) - sampleWidth;
            size_t mid_val = febitvec::s_get_uint(indexBase, mid_pos, sampleWidth);
            if (mid_val <= key)
                i = mid_idx + 1;
            else
                j = mid_idx;
        }
        if (terark_unlikely(loBlk == i)) {
            return lo;
        }
        i--;
    }
SearchBlock:
    assert(blockUnits <= 128);
    size_t block[128]; get_block(i, block);
    size_t l = (i+0 == loBlk) ? lo & mask : 0;
    size_t u = (i+1 == hiBlk) ? ((hi-1) & mask) + 1 : blockUnits;
    size_t k = upper_bound_n(block, l, u, key);
    size_t p = k + (i << m_log2_blockUnits);
    return p;
}

std::pair<size_t, size_t>
SortedUintVec::equal_range(size_t lo, size_t hi, size_t key) const {
    assert(lo <= hi);
    if (lo == hi) {
        return std::make_pair(lo, lo);
    }
    size_t log2 = m_log2_blockUnits;
    size_t blockUnits = size_t(1) << log2, mask = blockUnits - 1;
    size_t hiBlk = (hi + mask) >> log2;
    size_t loBlk = lo >> log2;
    size_t i = loBlk, j = hiBlk;
    if (loBlk + 1 < hiBlk) {
        auto indexBase = (const size_t* )(m_index);
        auto sampleWidth = m_sampleWidth;
        auto indexWidth = m_offsetWidth + sampleWidth;
        if (!m_is_samples_full_sorted) {
            size_t nextSample = s_get_block_min_val(
                indexBase, sampleWidth, indexWidth, loBlk+1);
            if (nextSample < key) {
                i = ++loBlk;
                lo = loBlk << log2;
            }
            else { // key <= nextSample
                auto loBase = lo & ~mask;
                if (nextSample == key) {
                    hi = upper_bound(loBase + blockUnits, hi, key);
                    // after calling upper_bound, to reduce stack space,
                    // because upper_bound also needs a block[128]
                    size_t block[128]; get_block(i, block);
                    size_t lo_k = lower_bound_n(block, lo & mask, blockUnits, key);
                    lo = loBase + lo_k;
                }
                else { // key < nextSample
                    size_t block[128]; get_block(i, block);
                    auto r = equal_range_n(block, lo & mask, blockUnits, key);
                    lo = loBase + r.first;
                    hi = loBase + r.second;
                }
                return std::make_pair(lo, hi);
            }
        }
        while (i < j) {
            size_t mid_idx = (i + j) / 2;
            size_t mid_pos = indexWidth * (mid_idx + 1) - sampleWidth;
            size_t mid_val = febitvec::s_get_uint(indexBase, mid_pos, sampleWidth);
            if (mid_val < key)
                i = mid_idx + 1;
            else if ( mid_val > key)
                j = mid_idx;
            else {
                size_t ii = i;
                {
                    size_t k = j;
                    while (i < k) {
                        mid_idx = (i + k) / 2;
                        mid_pos = indexWidth * (mid_idx + 1) - sampleWidth;
                        mid_val = febitvec::s_get_uint(indexBase, mid_pos, sampleWidth);
                        if (mid_val < key) // lower_bound
                            i = mid_idx + 1;
                        else
                            k = mid_idx;
                    }
                }
                {
                    size_t k = ii;
                    while (k < j) {
                        mid_idx = (k + j) / 2;
                        mid_pos = indexWidth * (mid_idx + 1) - sampleWidth;
                        mid_val = febitvec::s_get_uint(indexBase, mid_pos, sampleWidth);
                        if (mid_val <= key) // upper_bound
                            k = mid_idx + 1;
                        else
                            j = mid_idx;
                    }
                }
                break;
            }
        }
        if (terark_unlikely(j == loBlk)) {
            return std::make_pair(lo, lo);
        }
        if (i > loBlk) {
            i--;
        }
    }
    assert(i < j);
    assert(blockUnits <= 128);
    size_t block[128]; get_block(i, block);
    size_t hi_u = (j == hiBlk) ? ((hi-1) & (blockUnits-1)) + 1 : blockUnits;
    size_t lo_l = (i == loBlk) ? lo & (blockUnits-1) : 0;
    size_t lo_k, hi_k;
    j--;
    if (i < j) { // different blocks
        lo_k = lower_bound_n(block, lo_l, blockUnits, key);
        get_block(j, block);
        hi_k = upper_bound_n(block, 0, hi_u, key);
    }
    else {
        auto lohi = equal_range_n(block, lo_l, hi_u, key);
        lo_k = lohi.first;
        hi_k = lohi.second;
    }
    lo = lo_k + (i << log2);
    hi = hi_k + (j << log2);
    return std::make_pair(lo, hi);
}

void SortedUintVec::risk_set_data(const void* base, size_t bytes) {
	assert(m_is_sorted_uint_vec);
	auto oheader = (const ObjectHeader*)(base);
	size_t numUnits = oheader->units;
	size_t log2_blockUnits = oheader->log2_blockUnits;
	size_t offsetWidth = oheader->offsetWidth_1 + 1;
	size_t sampleWidth = oheader->sampleWidth_1 + 1;
	size_t blockUnits = size_t(1) << log2_blockUnits;
	if (64 == blockUnits || 128 == blockUnits) {
		// OK
	} else {
		THROW_STD(invalid_argument
			, "invalid log2_blockUnits = %zd, must be 6 or 7"
			  " (blockUnits must be 64 or 128)"
			, log2_blockUnits
		);
	}
	if (0 != m_size || !m_data.empty()) {
		THROW_STD(invalid_argument
			, "this function must be called on empty object"
		);
	}
	size_t indexEntries = ceiled_div(numUnits, blockUnits) + 1;
	size_t indexWidth   = offsetWidth + sampleWidth;
	size_t indexBits    = febitvec::align_bits(indexWidth * indexEntries);
	size_t indexBytes   = indexBits / 8;
	size_t indexOffset  = oheader->indexOffset;
	if (bytes < indexOffset + indexBytes) {
		THROW_STD(invalid_argument
			, "bytes = %zd is too small, indexOffset = %zd, indexBytes = %zd"
			, bytes, indexOffset, indexBytes
		);
	}
	m_data.risk_set_data((byte_t*)base, bytes);
	m_index = (const byte_t*)(base) + indexOffset;
	m_size = numUnits;
	m_log2_blockUnits = byte_t(log2_blockUnits);
	m_offsetWidth = byte_t(offsetWidth);
	m_sampleWidth = byte_t(sampleWidth);
	m_is_overall_full_sorted = oheader->is_overall_full_sorted;
	m_is_samples_full_sorted = oheader->is_samples_full_sorted;
}

void SortedUintVec::risk_release_ownership() {
	m_data.risk_release_ownership();
	clear();
}

void SortedUintVec::clear() {
	SortedUintVec().swap(*this);
}

void SortedUintVec::swap(SortedUintVec& y) {
	m_data.swap(y.m_data);
	std::swap(m_index          , y.m_index);
	std::swap(m_size           , y.m_size);
	std::swap(m_log2_blockUnits, y.m_log2_blockUnits);
	std::swap(m_offsetWidth    , y.m_offsetWidth    );
	std::swap(m_sampleWidth    , y.m_sampleWidth    );
#define DO_SWAP_BIT(x, y) {bool t = x; x = y; y = t;}
	DO_SWAP_BIT(m_is_sorted_uint_vec   , y.m_is_sorted_uint_vec);
	DO_SWAP_BIT(m_is_overall_full_sorted, y.m_is_overall_full_sorted);
	DO_SWAP_BIT(m_is_samples_full_sorted, y.m_is_samples_full_sorted);
#if TERARK_WORD_BITS == 64
	std::swap(m_padding        , y.m_padding);
#endif
}

/////////////////////////////////////////////////////////////////////////////
class SortedUintVec::Builder::Impl : public SortedUintVec::Builder {
	struct IndexItem {
		size_t  offset;
		size_t  sample;
	};
	valvec<byte_t>  m_data;
	valvec<int64_t> m_block;
	AutoFree<int64_t> m_diffvec;
	valvec<IndexItem> m_index;
	uint64_t m_lastValue;
	uint64_t m_maxValue;
	size_t   m_size;
	byte_t   m_log2_blockUnits;
	bool     m_is_sorted;
	bool     m_is_real_sorted;

	// m_smallToLarge[largeCount][smallWidth][largeWidth]
	AutoFree<unsigned[16][64]> m_smallToLarge;

    boost::intrusive_ptr<FileStream> m_fp;
    boost::intrusive_ptr<OutputBuffer> m_writer;
    size_t m_startPos;
	size_t m_indexOffset;

	size_t getBlockUnits() const {
		return m_block.capacity();
	}
	void inc_histogram(size_t largeCount, size_t smallWidth, size_t largeWidth) {
		if (m_smallToLarge) {
			size_t largeWidth_1 = largeWidth ? largeWidth-1 : 0;
			size_t smallWidthType = getWidthType(smallWidth, largeWidth);
			m_smallToLarge[largeCount][smallWidthType][largeWidth_1]++;
		}
	}
	void print_histogram() const;
	void append_block(uint64_t nextBlockFirstValue);
	void append_block_impl();
	void append_block_impl_lagrange(uint64_t nextBlockFirstValue);

    void init(size_t blockUnits);

public:
    Impl(size_t blockUnits, OutputBuffer* buffer, bool is_sorted);
    Impl(size_t blockUnits, fstring fpath, bool is_sorted);
	void push_back(uint64_t val) override;
	void append(const uint64_t* values, size_t count);
	void append(const valvec<uint64_t>& v) { append(v.data(), v.size()); }
    BuildResult finish(SortedUintVec* vec) override;
};

template<size_t BlockUnits>
static inline
size_t bits(size_t lo, size_t hi, const int64_t* ds) {
	size_t realsmallWidth = 1 + terark_bsr_u64(uint64_t(ds[hi-1] - ds[lo] + 1));
	size_t largeWidth = 1 + terark_bsr_u64(uint64_t(ds[BlockUnits-2]));
	size_t smallBits;
	size_t smallWidth = g_used_small_width[realsmallWidth];
	if (0 == smallWidth) {
		// this is very rare case!
		// zero means realSmallWidth == 1 and all values use largeWidth
		// effective Width is largeWidth+1
		return (largeWidth+1) * (BlockUnits-1);
	}
	if (64 % smallWidth == 0) {
		// last smallWidth bits can be used by largeUnits
		smallBits = smallWidth * (BlockUnits-1);
	} else {
		// tricky: for every 64 units, use uint64 words = smallWidth
		// and wasted bits == smallWidth
		smallBits = smallWidth * 64 * (BlockUnits/64);
	}
	size_t largeBits = largeWidth * (BlockUnits-1 - (hi-lo));
	return smallBits + largeBits;
}

template<size_t BlockUnits, size_t Dim>
static inline
void compute_lo_hi_index(const int64_t* ds, size_t* pLo, size_t* pHi) {
	uint32_t aa[Dim][Dim];
	for (size_t i = 0; i < Dim; ++i) {
		for (size_t j = 0; j < Dim; ++j)
			aa[i][j] = (uint32_t)bits<BlockUnits>(i, BlockUnits-1-j, ds);
	}
	size_t vMin = size_t(-1);
	size_t lo = size_t(-1), hi = size_t(-1);
	for (size_t i = 0; i < Dim; ++i) {
		for (size_t j = 0; j < Dim; ++j)
			if (vMin > aa[i][j])
				vMin = aa[i][j], lo = i, hi = j;
	}
	hi = BlockUnits-1-hi;
	*pLo = lo;
	*pHi = hi;
}

static inline
void compute_lo_hi_index(size_t blockUnits, const int64_t* ds,
						 size_t* pLo, size_t* pHi) {
	if (64 == blockUnits) {
		compute_lo_hi_index< 64, 32>(ds, pLo, pHi);
	} else if (128 == blockUnits) {
		compute_lo_hi_index<128, 64>(ds, pLo, pHi);
	} else {
		assert(0); // should not happen
		THROW_STD(invalid_argument,
			"bug: invalid blockUnits = %zd, should not happen", blockUnits);
	}
}

void SortedUintVec::Builder::Impl::append_block(uint64_t nextBlockFirstValue) {
	size_t const old_data_size = m_data.size();
	size_t const blockUnits = getBlockUnits();
	auto vals = m_block.data();
	bool isIncreasing = true;
	for (size_t i = 1; i < blockUnits; ++i) {
		if (vals[i-1] > vals[i]) {
			isIncreasing = false;
			break;
		}
	}
	if (isIncreasing) {
		append_block_impl();
	} else {
		append_block_impl_lagrange(nextBlockFirstValue);
	}
	m_indexOffset += m_data.size() - old_data_size;
	if (m_writer) {
		assert(sizeof(ObjectHeader) == old_data_size);
		auto blockData = m_data.data() + sizeof(ObjectHeader);
		auto blockSize = m_data.size() - sizeof(ObjectHeader);
		m_writer->ensureWrite(blockData, blockSize);
		m_data.risk_set_size(sizeof(ObjectHeader));
	}
	else {
		assert(m_data.size() == m_indexOffset);
	}
}

void SortedUintVec::Builder::Impl::append_block_impl() {
	const size_t blockUnits = getBlockUnits();
	auto vals = m_block.data();
	auto ds = m_diffvec.p;
	m_index.push_back({m_indexOffset - sizeof(ObjectHeader), (size_t)vals[0]});
	for (size_t i = 1; i < blockUnits; ++i) {
		ds[i-1] = vals[i] - vals[i-1];
	}
	std::sort(ds, ds+blockUnits-1); // sort the diff sequence
	if (ds[0] == ds[blockUnits-2]) {
		WriteHeader(&m_data, ds[0], 0, 0);
		inc_histogram(0, 0, 0);
		return;
	}
	if (ds[0] + 1 == ds[blockUnits-2]) {
		int64_t loWater = ds[0];
		WriteHeader(&m_data, loWater, 1, 1);
		inc_histogram(0, 1, 1);
		// re-generate the diff sequence
		for (size_t i = 1; i < blockUnits; ++i) {
			ds[i-1] = vals[i] - vals[i-1];
		}
		ds[blockUnits-1] = loWater; // to zero last unused bit
		// do not need to store large units
		for (size_t i = 0; i < blockUnits / 64; ++i) {
			uint64_t w = 0;
			auto pds = ds + i * 64;
			for (size_t j = 0; j < 64; j++) {
				w |= (pds[j] - loWater) << j; // don't need +1
			}
			m_data.append((byte_t*)&w, 8);
		}
		return;
	}
	if (ds[0] + 3 >= ds[blockUnits-2]) {
		int64_t loWater = ds[0];
		WriteHeader(&m_data, loWater, 2, 2);
		inc_histogram(0, 2, 2);
		// re-generate the diff sequence
		for (size_t i = 1; i < blockUnits; ++i) {
			ds[i-1] = vals[i] - vals[i-1];
		}
		ds[blockUnits-1] = loWater; // to zero last unused unit
		// do not need to store large units
		for (size_t i = 0; i < blockUnits / 32; ++i) {
			uint64_t w = 0;
			auto pds = ds + i * 32;
			for (size_t j = 0; j < 32; j++) {
				w |= (pds[j] - loWater) << (2*j); // don't need +1
			}
			m_data.append((byte_t*)&w, 8);
		}
		return;
	}
	size_t lo, hi;
	compute_lo_hi_index(blockUnits, ds, &lo, &hi);
	int64_t loWater = ds[lo], hiWater = ds[hi-1];
	size_t realsmallWidth = 1 + terark_bsr_u64(uint64_t(hiWater - loWater + 1));
	size_t largeWidth = 1 + terark_bsr_u64(uint64_t(ds[blockUnits-2]));
	assert(realsmallWidth <= largeWidth); // may be equal
	assert(largeWidth >= 2);
	size_t smallWidth = g_used_small_width[realsmallWidth];
	inc_histogram(blockUnits-1-(hi-lo), smallWidth, largeWidth);
	// re-generate the diff sequence
	for (size_t i = 1; i < blockUnits; ++i) {
		ds[i-1] = vals[i] - vals[i-1];
	}
	WriteHeader(&m_data, loWater, smallWidth, largeWidth);
	size_t wordUnits = 64/smallWidth;
	size_t smallOffset = m_data.size();
	size_t largeBitPos, largeBytes, largeUnits = 0;
	if (64 % smallWidth == 0) {

		// to set last small unit
		// last small unit will always be overwritten
		//
		// set it to loWater will mark it as 'smallUnit'
		// and 'largeUnits' will not be over increased
		ds[blockUnits-1] = loWater;

		for(size_t i = 0; i < blockUnits / wordUnits; ++i) {
			auto pds = ds + i * (64 / smallWidth);
			uint64_t w = 0;
			for (size_t j = 0; j < wordUnits; ++j) {
				if (pds[j] >= loWater && pds[j] <= hiWater)
					w |= (pds[j] - loWater + 1) << (smallWidth*j);
				else
					largeUnits++;
			}
			m_data.append((byte_t*)&w, 8);
		}
		largeBitPos = smallWidth * (blockUnits - 1);
		// largeWidth use 6 bits:
		largeBytes = (largeWidth * largeUnits - smallWidth + 6 + 7) / 8;
	}
	else {
		uint64_t encoded[44];
		size_t baseWords = smallWidth * (blockUnits/64); // tricky!
		assert(baseWords <= 40);
		size_t virtualWords = (blockUnits-1 + wordUnits-1) / wordUnits;
		assert(virtualWords <= 44);
		for(size_t i = 0; i < virtualWords; ++i) {
			auto pds = ds + i * wordUnits;
			size_t n;
			if (i < virtualWords-1 || (blockUnits-1)%wordUnits == 0)
				n = wordUnits;
			else
				n = (blockUnits-1) % wordUnits;
			uint64_t w = 0;
			for (size_t j = 0; j < n; ++j) {
				if (pds[j] >= loWater && pds[j] <= hiWater)
					w |= (pds[j] - loWater + 1) << (smallWidth * j);
				else
					largeUnits++;
			}
			encoded[i] = w;
		}
		size_t baseBitWidth = smallWidth * wordUnits;
		size_t extrBitWidth = 64 - baseBitWidth;
		size_t extrBitPos = 0;
		uint64_t extrBitsPacked[8];
		// high extrBitWidth bits are hole bits in each word
		// pack such extra bits to extrBitsPacked makes no holes
		for(size_t i = baseWords; i < virtualWords; ++i) {
			uint64_t w = encoded[i];
			assert(w <= uint64_t(-1) >> extrBitWidth);
			assert(extrBitPos + baseBitWidth <= sizeof(extrBitsPacked)*8);
			febitvec::s_set_uint(extrBitsPacked, extrBitPos, baseBitWidth, w);
			extrBitPos += baseBitWidth;
			// extrBitPos may exceeds real bitpos, but this is ok
			// because exceeded bits are all zero
		}
		extrBitPos = 0;
		for(size_t i = 0; i < baseWords; ++i) {
			uint64_t extrBits = febitvec::s_get_uint(extrBitsPacked, extrBitPos, extrBitWidth);
			encoded[i] |= extrBits << baseBitWidth;
			extrBitPos += extrBitWidth;
		}
		m_data.append((byte_t*)encoded, 8 * baseWords);
		largeBitPos = 64 * baseWords;
		// largeWidth use 6 bits:
		largeBytes = (largeWidth * largeUnits + 6 + 7) / 8;
	}
	if (largeUnits) {
		m_data.grow(largeBytes + 7); // 7 bytes extra for safe uint64
		m_data.pop_n(7); // return back 7 bytes
		largeBitPos += 8 * smallOffset;
		uint64_t* pData = (uint64_t*)(m_data.data());
		febitvec::s_set_uint(pData, largeBitPos, 6, largeWidth - smallWidth);
		largeBitPos += 6;
		for (size_t i = 0; i < blockUnits-1; ++i) {
			if (ds[i] < loWater || ds[i] > hiWater) {
				febitvec::s_set_uint(pData, largeBitPos, largeWidth, ds[i]);
				largeBitPos += largeWidth;
			}
		}
	} else {
		TERARK_IF_DEBUG(int xxx = 0; TERARK_UNUSED_VAR(xxx);,;);
	}
}

void SortedUintVec::Builder::Impl::append_block_impl_lagrange(uint64_t nextBlockFirstValue) {
    const size_t log2_range = m_log2_blockUnits;
    const size_t blockUnits = getBlockUnits();
    auto vals = m_block.data();
    m_index.push_back({ m_indexOffset - sizeof(ObjectHeader), (size_t)vals[0] });
    int64_t a = 0;
    int64_t b = blockUnits;
    int64_t m = (b-a)/2;
    int64_t f_a = 0;
    int64_t f_b = nextBlockFirstValue - vals[0];
    int64_t f_m = vals[m] - vals[0];
    int64_t f_m_encoded = encode_signed(f_m - (f_a + f_b) / 2);
    auto fix = (uint64_t*)m_diffvec.p;
    auto set_fix = [=](size_t idx, size_t x) {
        int64_t f_x_lag = lagrange(x, a, f_a, m, f_m, b, f_b, log2_range);
        int64_t f_x_real = vals[x] - vals[0];
        int64_t diff = f_x_real - f_x_lag;
    //  printf("build: x = %3zd , f_x_lag = %3zd , diff = %3zd\n", x, f_x_lag, diff);
        fix[idx] = encode_signed(diff);
    };
    for (size_t i = 1; i < blockUnits / 2; ++i) {
        set_fix(i - 1, i);
    }
    for (size_t i = blockUnits / 2 + 1; i < blockUnits; ++i) {
        set_fix(i - 2, i);
    }
//  uint64_t minFix = fix[0];
    uint64_t maxFix = fix[0];
    for (size_t i = 1; i < blockUnits - 2; ++i) {
//      if (minFix > fix[i]) minFix = fix[i];
        if (maxFix < fix[i]) maxFix = fix[i];
    }
    WriteHeader(&m_data, f_m_encoded, 2, 2); // special (2,2)
//  size_t f_width = (maxFix - minFix) < 4
//                 ? 2 : terark_bsr_u64(maxFix - minFix) + 1;
    size_t f_width = maxFix < 4 ? 2 : terark_bsr_u64(maxFix) + 1;
    size_t bitpos = m_data.size() * 8;
    m_data.grow(ceiled_div(6 + f_width*(blockUnits - 2), 8));
    uint64_t* pData = (uint64_t*)(m_data.data());
    febitvec::s_set_uint(pData, bitpos, 6, f_width - 1);
    bitpos += 6;
    for (size_t i = 0; i < blockUnits - 2; ++i) {
    //  uint64_t val = fix[i] - minFix; // !!don't use minFix
        uint64_t val = fix[i];
        febitvec::s_set_uint(pData, bitpos, f_width, val);
        bitpos += f_width;
    }
    assert(bitpos <= m_data.size() * 8);
}

void SortedUintVec::Builder::Impl::init(size_t blockUnits) {
    if (64 != blockUnits && 128 != blockUnits) {
        THROW_STD(invalid_argument
            , "invalid blockUnits = %zd, must be 64 or 128"
            , blockUnits);
    }
    //	if (fast_popcount(blockUnits) != 1) {
    //		THROW_STD(invalid_argument,
    //			"invalid blockUnits = %zd", blockUnits);
    //	}
    m_data.reserve(4096);
    m_data.grow(sizeof(ObjectHeader));
    m_index.reserve(256);
    m_lastValue = 0;
    m_maxValue = 0;
    m_size = 0;
    m_block.reserve(blockUnits);
    m_diffvec.alloc(blockUnits); // has 1 extra unit
    m_log2_blockUnits = terark_bsr_u64(blockUnits);

    if (getEnvBool("SortedUintVec_Builder_EnableHistogram")) {
        m_smallToLarge.alloc(blockUnits);
        memset(m_smallToLarge, 0, sizeof(*m_smallToLarge)*blockUnits);
    }

    m_indexOffset = sizeof(ObjectHeader);
}

SortedUintVec::Builder::Impl::Impl(size_t blockUnits, OutputBuffer* buffer, bool is_sorted) {
    init(blockUnits);
    m_is_sorted = is_sorted;
    m_is_real_sorted = true;
    // m_fp should not assign
    // for buffer->getOutputStream() can be stack object
    if (buffer) {
        auto fpSeekable = dynamic_cast<ISeekable*>(buffer->getOutputStream());
        assert(fpSeekable != nullptr);
        m_writer.reset(buffer);
        m_startPos = fpSeekable->tell() + m_writer->bufpos();
        m_writer->ensureWrite(m_data.data(), sizeof(ObjectHeader)); // all zero
    }
}

SortedUintVec::Builder::Impl::Impl(size_t blockUnits, fstring fpath, bool is_sorted) {
    init(blockUnits);
    m_is_sorted = is_sorted;
    m_is_real_sorted = true;
    m_fp.reset(new FileStream(fpath.c_str(), "wb+"));
    m_writer.reset(new OutputBuffer(m_fp.get()));
    m_startPos = 0;
    m_writer->ensureWrite(m_data.data(), sizeof(ObjectHeader)); // all zero
}

void SortedUintVec::Builder::Impl::push_back(uint64_t val) {
	const size_t blockUnits = getBlockUnits();
	if (terark_unlikely(m_block.size() >= blockUnits)) {
		if (terark_unlikely(m_block.size() > blockUnits)) {
			THROW_STD(invalid_argument
				, "blockUnits = %zd, but m_block.size = %zd"
				,  blockUnits, m_block.size()
			);
		}
		append_block(val);
		m_block.risk_set_size(0);
	}
	if (terark_unlikely(m_block.capacity() < blockUnits)) {
		m_block.reserve(blockUnits);
	}
	if (val < m_lastValue) {
		if (m_is_sorted) {
			THROW_STD(invalid_argument
				, "values must be in ascending order, m_lastValue = %llu, cur = %llu"
				,  ullong(m_lastValue), ullong(val)
			);
		}
		m_is_real_sorted = false;
	}
	else if (terark_unlikely(m_size > 0 && val - m_lastValue >= uint64_t(1)<<(20+31))) {
		THROW_STD(invalid_argument
			, "diff = 0x%llX is larger than 51 bits, m_lastValue = %llu, cur = %llu"
			,  ullong(val - m_lastValue), ullong(m_lastValue), ullong(val)
		);
	}
	m_maxValue = std::max(m_maxValue, val);
	m_block.unchecked_push_back(val);
	m_size++;
	m_lastValue = val;
}

void SortedUintVec::Builder::Impl::append(const uint64_t* values, size_t count) {
	const size_t blockUnits = getBlockUnits();
	if (terark_unlikely(count != blockUnits)) {
		THROW_STD(invalid_argument
			, "blockUnits = %zd, but argument block.size = %zd"
			,  blockUnits, count
		);
	}
	for (size_t i = 0; i < count; ++i) {
		push_back(values[i]);
	}
}

//#ifdef __GNUC__
//#   pragma GCC push_options
//#   pragma GCC optimize("-fno-tree-partial-pre")
//    // -ftree-partial-pre makes bad_alloc exception
//#endif

SortedUintVec::Builder::BuildResult
SortedUintVec::Builder::Impl::finish(SortedUintVec* vec) {
    const size_t blockUnits = getBlockUnits();
    if (m_block.size()) {
        while (m_block.size() < blockUnits)
            m_block.unchecked_push_back(m_lastValue);
        append_block(m_lastValue);
    }
    m_index.push_back({ m_indexOffset - sizeof(ObjectHeader), (size_t)m_lastValue });
    m_data.append("\0\0\0\0\0\0\0", 7); // zero padding
    if (m_writer) {
        while ((m_indexOffset + m_data.size()) % 8 != 0) { // align padding
            m_data.push_back(0);
        }
        auto pad_data = m_data.data() + sizeof(ObjectHeader);
        auto pad_size = m_data.size() - sizeof(ObjectHeader);
        m_writer->ensureWrite(pad_data, pad_size);
        m_indexOffset += pad_size;
#if !defined(NDEBUG)
        auto fpSeekable = dynamic_cast<ISeekable*>(m_writer->getOutputStream());
        assert(!m_fp || dynamic_cast<FileStream*>(fpSeekable) == m_fp.get());
        assert(size_t(fpSeekable->tell()) + m_writer->bufpos() == m_startPos + m_indexOffset);
#endif
        m_data.risk_set_size(sizeof(ObjectHeader));
    }
    else {
        while (m_data.size() % 8 != 0) { // align padding
            m_data.push_back(0);
        }
        m_indexOffset = m_data.size();
    }
    bool is_samples_full_sorted = true;
    if (!m_is_real_sorted) {
        for (size_t i = 1; i < m_index.size(); ++i) {
            if (m_index[i-1].sample > m_index[i].sample) {
                is_samples_full_sorted = false;
                break;
            }
        }
    }
    else {
        assert(m_lastValue == m_maxValue);
    }
    size_t numBlocks = (m_size + blockUnits - 1) / blockUnits;
    assert(numBlocks + 1 == m_index.size());

    // m_maxValue==0 cause terark_bsr_u64 yields undefined value
    // so use safe_bsr_u64
    size_t offsetWidth = 1 + safe_bsr_u64(m_indexOffset - sizeof(ObjectHeader));
    size_t sampleWidth = 1 + safe_bsr_u64(m_maxValue);

    size_t indexWidth = offsetWidth + sampleWidth;
    m_data.grow(ceiled_div(indexWidth*(numBlocks + 1), 8));
    BuildResult result;
    if (m_writer) {
        while ((m_indexOffset + m_data.size()) % 16 != 0) { // align padding
            m_data.push_back(0);
        }
        result.mem_size = m_indexOffset + m_data.size();
    }
    else {
        while (m_data.size() % 16 != 0) { // align padding
            m_data.push_back(0);
        }
        result.mem_size = m_data.size();
    }
    m_data.shrink_to_fit();
  {
    IndexItem* rawIndex = m_index.data();
    uint64_t * zipIndex;
    if (m_writer) {
        zipIndex = (uint64_t*)(m_data.data() + sizeof(ObjectHeader));
    }
    else {
        zipIndex = (uint64_t*)(m_data.data() + m_indexOffset);
    }
    for(size_t bitpos = 0, i = 0; i < numBlocks + 1; ++i) {
        size_t offset = rawIndex[i].offset;
        size_t sample = rawIndex[i].sample;
        febitvec::s_set_uint(zipIndex, bitpos, offsetWidth, offset); bitpos += offsetWidth;
        febitvec::s_set_uint(zipIndex, bitpos, sampleWidth, sample); bitpos += sampleWidth;
    }
  }
#if !defined(NDEBUG) && 0
    for (size_t bitpos = 0, i = 0; i < numBlocks + 1; ++i) {
        size_t offset = rawIndex[i].offset;
        size_t sample = rawIndex[i].sample;
        size_t offset2 = febitvec::s_get_uint(zipIndex, bitpos, offsetWidth); bitpos += offsetWidth;
        size_t sample2 = febitvec::s_get_uint(zipIndex, bitpos, sampleWidth); bitpos += sampleWidth;
        assert(offset2 == offset);
        assert(sample2 == sample);
    }
#endif
    auto oheader = (ObjectHeader*)m_data.data();
    oheader->units = m_size;
    oheader->log2_blockUnits = m_log2_blockUnits;
    oheader->offsetWidth_1 = offsetWidth - 1;
    oheader->sampleWidth_1 = sampleWidth - 1;
    oheader->indexOffset = m_indexOffset;
    oheader->is_samples_full_sorted = is_samples_full_sorted;
    oheader->is_overall_full_sorted = m_is_real_sorted;
    if (m_writer) {
        // m_fp   = header + data + index
        // m_data = header        + index
        //                  ^^^^^^
        //                  data is not in memory
        m_writer->ensureWrite(oheader + 1, m_data.size() - sizeof(ObjectHeader));
        m_writer->flush_buffer();
        auto fpOutput = m_writer->getOutputStream();
        auto fpInput = dynamic_cast<IInputStream*>(fpOutput);
        auto fpSeekable = dynamic_cast<ISeekable*>(fpOutput);
        fpOutput->flush(); // must flush, VC bug?
        auto endPos = fpSeekable->tell();
        fpSeekable->seek(m_startPos);
        m_writer->ensureWrite(oheader, sizeof(ObjectHeader));
        m_writer->flush_buffer();
        fpOutput->flush(); // must flush, VC bug?
        if (vec) {
            size_t indexSize = m_data.size() - sizeof(ObjectHeader);
            size_t  dataSize = m_indexOffset - sizeof(ObjectHeader);
            m_data.grow_no_init(dataSize);
            auto base = m_data.data();
            memmove(base + m_indexOffset, base + sizeof(ObjectHeader), indexSize);
            auto readSize = fpInput->read(base + sizeof(ObjectHeader), dataSize);
            TERARK_UNUSED_VAR(readSize);
            assert(readSize == dataSize); 
            assert(dataSize % 8 == 0);
        }
        fpSeekable->seek(endPos);
        m_writer.reset();
        m_fp.reset();
    }
    else {
        if (NULL == vec) {
            THROW_STD(invalid_argument, "If without file, vec must not be NULL");
        }
    }
    if (vec) {
        vec->m_size = m_size;
        vec->m_data.swap(m_data);
        vec->m_index = vec->m_data.data() + m_indexOffset;
        vec->m_log2_blockUnits = m_log2_blockUnits;
        vec->m_offsetWidth = offsetWidth;
        vec->m_sampleWidth = sampleWidth;
        vec->m_is_overall_full_sorted = m_is_real_sorted;
        vec->m_is_samples_full_sorted = is_samples_full_sorted;
    }
    if (m_smallToLarge) {
        print_histogram();
    }
    result.size = m_size;
    return result;
}

//#ifdef __GNUC__
//#   pragma GCC pop_options
//#endif

void SortedUintVec::Builder::Impl::print_histogram() const {
	if (!m_smallToLarge) {
		fprintf(stderr, "ERROR: SortedUintVec::Builder::Impl: histogram is not enabled\n");
		return;
	}
	// wt: widthType
	// lw: largeWidth-1
	// lc: largeCount
	printf("wt lw\t(lc   freq)+\n");
	printf(" 0  0\t  0 %6u\n", m_smallToLarge[0][0][0]);
	const size_t blockUnits = getBlockUnits();
	for(size_t wt = 1; wt < 16; ++wt) {
		for (size_t lw = 0; lw < 64; ++lw) {
			size_t cn = 0;
			for (size_t lc = 0; lc < blockUnits; ++lc) {
				cn += m_smallToLarge[lc][wt][lw];
			}
			if (cn) {
				printf("%2zd %2zd", wt, lw+1);
				for (size_t lc = 0; lc < blockUnits; ++lc) {
					if (unsigned freq = m_smallToLarge[lc][wt][lw]) {
						printf("\t%3zd %6u", lc, freq);
					}
				}
				printf("\n");
			}
		}
	}
}

///////////////////////////////////////////////////////////////////////

SortedUintVec::Builder::~Builder() {}

SortedUintVec::Builder*
SortedUintVec::createBuilder(size_t blockUnits) {
	return createBuilder(true, blockUnits);
}

SortedUintVec::Builder*
SortedUintVec::createBuilder(size_t blockUnits, const char* fname) {
	return createBuilder(true, blockUnits, fname);
}

SortedUintVec::Builder*
SortedUintVec::createBuilder(size_t blockUnits, OutputBuffer* buffer) {
	return createBuilder(true, blockUnits, buffer);
}

SortedUintVec::Builder*
SortedUintVec::createBuilder(bool inputSorted, size_t blockUnits) {
	return new Builder::Impl(blockUnits, nullptr, inputSorted);
}

SortedUintVec::Builder*
SortedUintVec::createBuilder(bool inputSorted, size_t blockUnits, const char* fname) {
	if (NULL == fname)
		return new Builder::Impl(blockUnits, nullptr, inputSorted);
	return new Builder::Impl(blockUnits, fname, inputSorted);
}

SortedUintVec::Builder*
SortedUintVec::createBuilder(bool inputSorted, size_t blockUnits, OutputBuffer* buffer) {
	return new Builder::Impl(blockUnits, buffer, inputSorted);
}

} // namespace terark
