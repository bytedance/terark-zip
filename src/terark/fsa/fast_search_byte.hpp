#ifndef __terark_fsa_fast_search_byte_hpp__
#define __terark_fsa_fast_search_byte_hpp__

#include <immintrin.h>
#include <terark/succinct/rank_select_basic.hpp>

namespace terark {

inline size_t
binary_search_byte(const byte_t* data, size_t len, byte_t key) {
	size_t lo = 0;
	size_t hi = len;
	while (lo < hi) {
		size_t mid = (lo + hi) / 2;
		if (data[mid] < key)
			lo = mid + 1;
		else
			hi = mid;
	}
	if (lo < len && data[lo] == key)
		return lo;
	else
		return len;
}

#if defined(__SSE4_2__) // && (defined(NDEBUG) || !defined(__GNUC__))
	inline int // _mm_cmpestri length param is int32
	sse4_2_search_byte(const byte_t* data, int len, byte_t key) {
		// intrinsic: _mm_cmpestri can not generate "pcmpestri xmm, mem, imm"
		__m128i key128 = { char(key) }; // sizeof(__m128i)==16
		//-----------------^^^^----- to avoid vc warning C4838: conversion from 'byte_t' to 'char'
#if defined(__GNUC__)
		// gcc 7.1 typedef'ed such a type named __m128i_u
		// my_m128i_u has been proved work on gcc 4.8+
		typedef long long my_m128i_u __attribute__((__vector_size__(16), __may_alias__, __aligned__(1)));
#else
		typedef __m128i my_m128i_u;
#endif
	#if 0
		int pos;
		for(pos = 0; pos < len-16; pos += 16) {
		//	memcpy(&str128, data + pos, 16); // load
			int idx = _mm_cmpestri(key128, 1,
				*(const my_m128i_u*)(data+pos), 16, // don't require memory align
			//	str128, 16,
				_SIDD_UBYTE_OPS|_SIDD_CMP_EQUAL_ORDERED|_SIDD_LEAST_SIGNIFICANT);
			if (idx < 16)
				return pos + idx;
		}
	#else
		assert(len <= 16);
		int const pos = 0;
	#endif
	//	memcpy(&str128, data + pos, 16); // load
		int idx = _mm_cmpestri(key128, 1,
			*(const my_m128i_u*)(data+pos), len-pos, // don't require memory align
		//	str128, len-pos,
			_SIDD_UBYTE_OPS|_SIDD_CMP_EQUAL_ORDERED|_SIDD_LEAST_SIGNIFICANT);
	#if 0
		if (idx < len-pos)
	//	if (idx < 16) // _mm_cmpestri returns 16 when not found
			return pos + idx;
		else
			return len;
	#else
		// if search failed, return value will >= len
		// so the above check for idx is not needed
		// the caller will check if the return value is >= len or < len
		return pos + idx;
	#endif
	}
	inline size_t
	fast_search_byte(const byte_t* data, size_t len, byte_t key) {
		if (len <= 16)
			return sse4_2_search_byte(data, int(len), key);
		else
			return binary_search_byte(data, len, key);
	}
	inline size_t
	fast_search_byte_max_35(const byte_t* data, size_t len, byte_t key) {
		assert(len <= 35);
		if (len <= 16) {
			return sse4_2_search_byte(data, int(len), key);
		}
		size_t pos = sse4_2_search_byte(data, 16, key);
		if (pos < 16) {
			return pos;
		}
		if (len <= 32) {
			return 16 + sse4_2_search_byte(data + 16, int(len - 16), key);
		}
		pos = sse4_2_search_byte(data + 16, 16, key);
		if (pos < 16) {
			return 16 + pos;
		}
		return 32 + sse4_2_search_byte(data + 32, int(len - 32), key);
	}
	#define fast_search_byte_max_16 sse4_2_search_byte
#else
	#define fast_search_byte binary_search_byte
	#define fast_search_byte_max_16 binary_search_byte
	#define fast_search_byte_max_35 binary_search_byte
#endif
/*
	inline size_t
	fast_search_byte_rs_seq(const byte_t* data, size_t len, byte_t key) {
		assert(len >= 32);
		size_t pc = 0;
		for(size_t i = 0; i < 256/TERARK_WORD_BITS; ++i) {
			size_t w = unaligned_load<size_t>(data + i*sizeof(size_t));
			if (i*TERARK_WORD_BITS < key) {
				pc += fast_popcount(w);
			} else {
				pc += fast_popcount_trail(w, key % TERARK_WORD_BITS);
				break;
			}
		}
		return pc;
	}
*/

	inline size_t
	popcount_rs_256(const byte_t* data) {
		size_t w = unaligned_load<uint64_t>(data + 4, 3);
		return data[3] + fast_popcount(w);
	}

	inline size_t
	fast_search_byte_rs_idx(const byte_t* data, byte_t key) {
		size_t i = key / TERARK_WORD_BITS;
		size_t w = unaligned_load<size_t>(data + 4 + i*sizeof(size_t));
		size_t b = data[i];
		return b + fast_popcount_trail(w, key % TERARK_WORD_BITS);
	}

	inline size_t
	fast_search_byte_rs_idx(const byte_t* data, size_t len, byte_t key) {
		if (terark_unlikely(!terark_bit_test((size_t*)(data + 4), key)))
			return len;
		assert(len >= 36);
		size_t i = key / TERARK_WORD_BITS;
		size_t w = unaligned_load<size_t>(data + 4 + i*sizeof(size_t));
		size_t b = data[i];
		return b + fast_popcount_trail(w, key % TERARK_WORD_BITS);
	}

	inline size_t
	fast_search_byte_may_rs(const byte_t* data, size_t len, byte_t key) {
		if (len < 36)
			return fast_search_byte_max_35(data, len, key);
		else
			return fast_search_byte_rs_idx(data, len, key);
	}

    inline size_t rs_next_one_pos(const void* rs, size_t ch) {
        size_t i = ch/64;
        uint64_t w = unaligned_load<uint64_t>(rs, i);
        w >>= (ch & 63);
        w >>= 1;
        if (w) {
            return ch + 1 + fast_ctz64(w);
        }
        else {
            assert(i < 3);
            do w = unaligned_load<uint64_t>(rs, ++i); while (0==w);
            assert(i < 4);
            return i * 64 + fast_ctz64(w);
        }
    }

    inline size_t rs_prev_one_pos(const void* rs, size_t ch) {
        size_t i = ch/64;
        uint64_t w = unaligned_load<uint64_t>(rs, i);
        w <<= 63 - (ch & 63);
        w <<= 1;
        if (w) {
            return ch - 1 - fast_clz64(w);
        }
        else {
            assert(i > 0);
            do w = unaligned_load<uint64_t>(rs, --i); while (0==w);
            assert(i < 3); // i would not be wrapped back to size_t(-1, -2, ...)
            return i * 64 + terark_bsr_u64(w);
        }
    }

    inline size_t rs_select1(const byte_t* rs, size_t rank1) {
        assert(rs[0] == 0); // rs[0] is always 0
        if (rank1 < rs[2]) {
            if (rank1 < rs[1]) {
                uint64_t w = unaligned_load<uint64_t>(rs + 4);
                return 0*64 + UintSelect1(w, rank1);
            }
            else {
                uint64_t w = unaligned_load<uint64_t>(rs + 4 + 8);
                return 1*64 + UintSelect1(w, rank1 - rs[1]);
            }
        }
        else {
            if (rank1 < rs[3]) {
                uint64_t w = unaligned_load<uint64_t>(rs + 4 + 16);
                return 2*64 + UintSelect1(w, rank1 - rs[2]);
            }
            else {
                uint64_t w = unaligned_load<uint64_t>(rs + 4 + 24);
                return 3*64 + UintSelect1(w, rank1 - rs[3]);
            }
        }
    }


} // namespace terark

#endif // __terark_fsa_fast_search_byte_hpp__

