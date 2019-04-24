/*
 * rank_select_inline.hpp
 *
 *  Created on: Sep 1, 2015
 *      Author: leipeng
 */
#ifndef TERARK_RANK_SELECT_INLINE_BMI2_HPP_
#define TERARK_RANK_SELECT_INLINE_BMI2_HPP_

#include <terark/config.hpp>
#include <terark/util/throw.hpp>

#if !defined(__BMI2__)
    #error __BMI2__ must be defined
#endif

#if defined(__GNUC__) && __GNUC__*1000 + __GNUC_MINOR__ < 4008 \
    && !defined(__clang__) && !defined(__INTEL_COMPILER)
    #include <bmiintrin.h>
    /* Intel-specified, single-leading-underscore version of BEXTR */
    static __inline__ unsigned int __attribute__((__always_inline__))
    _bextr_u32(unsigned int __X, unsigned int __Y, unsigned int __Z) {
      return __builtin_ia32_bextr_u32 (__X, ((__Y & 0xff) | ((__Z & 0xff) << 8)));
    }
    /* Intel-specified, single-leading-underscore version of BEXTR */
    static __inline__ unsigned long long __attribute__((__always_inline__))
    _bextr_u64(unsigned long long __X, unsigned int __Y, unsigned int __Z) {
      return __builtin_ia32_bextr_u64 (__X, ((__Y & 0xff) | ((__Z & 0xff) << 8)));
    }
#endif

namespace terark {

inline size_t UintSelect1(unsigned int x, size_t r) {
    assert(x != 0);
    assert(r < (size_t)_mm_popcnt_u32(x));
    return terark_bsr_u32(_pdep_u32(_bzhi_u32(uint32_t(-1), r+1), x));
}
inline size_t UintSelect1(unsigned long long x, size_t r) {
    assert(x != 0);
  #if TERARK_WORD_BITS >= 64
    assert(r < (size_t)_mm_popcnt_u64(x));
    return terark_bsr_u64(_pdep_u64(_bzhi_u64(uint64_t(-1), r+1), x));
  #else
    uint32_t lo32 = (uint32_t)(x);
    uint32_t lo32pc = _mm_popcnt_u32(lo32);
    if (r < lo32pc) {
        return terark_bsr_u32(_pdep_u32(_bzhi_u32(uint32_t(-1), r+1), lo32));
    }
    else {
        uint32_t hi32 = (uint32_t)(x >> 32);
        unsigned hi_r = r - lo32pc;
        assert(r < 32 + (size_t)_mm_popcnt_u32(hi32));
        return 32 + terark_bsr_u32(_pdep_u32(_bzhi_u32(uint32_t(-1), hi_r+1), hi32));
    }
  #endif
}

inline size_t UintSelect1(unsigned long x, size_t r) {
  #if ULONG_MAX == 0xFFFFFFFF
    return UintSelect1((unsigned int)x, r);
  #else
    return UintSelect1((unsigned long long)x, r);
  #endif
}

// 'k' may be 0
#if defined(__GNUC__) || 1
// g++-4.9 on cywin is faster than using _bextr_u64
// on msvc is also faster than using _bextr_u64
#define TERARK_GET_BITS_64(u64,k,width) ( k ? (u64 >> (k-1)*width) & ((1<<width)-1) : 0 )
#else
#define TERARK_GET_BITS_64(u64, k, width) _bextr_u64(u64, (k-1)*width, width)
#endif


} // namespace terark



#endif /* TERARK_RANK_SELECT_INLINE_BMI2_HPP_ */
