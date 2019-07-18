#pragma once
#include <terark/config.hpp>
#include <xmmintrin.h>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#if defined(__GNUC__) && __GNUC__ * 1000 + __GNUC_MINOR__ >= 4005
	#include <x86intrin.h>
#endif
#if defined(_MSC_VER) && _MSC_VER >= 1500 || defined(__CYGWIN__)
	#include <intrin.h>
#endif

#if defined(__GNUC__)
    #define TERARK_GNU_UNALIGNED  __attribute__((__may_alias__, __aligned__(1)))
#else
    #define TERARK_GNU_UNALIGNED
#endif

namespace terark {

static inline //HSM_FORCE_INLINE
byte_t* small_memcpy_align_1(void* dst, const void* src, size_t len) {
	typedef uint64_t   By8 TERARK_GNU_UNALIGNED;
	typedef uint32_t   By4 TERARK_GNU_UNALIGNED;
	typedef uint16_t   By2 TERARK_GNU_UNALIGNED;
	typedef uint8_t    By1;
    auto bdst = (      By1*)dst;
    auto bsrc = (const By1*)src;
    while (len >= 16) {
	    __m128i m0 = _mm_loadu_si128((const __m128i*)bsrc);
	    _mm_storeu_si128((__m128i*)bdst, m0);
        len  -= 16;
        bsrc += 16;
        bdst += 16;
    }
    bsrc += len;
    bdst += len;
    switch (len) {
    case 15: *(By8*)(bdst - 15) = *(const By8*)(bsrc - 15); no_break_fallthrough;
    case  7: *(By4*)(bdst -  7) = *(const By4*)(bsrc -  7); no_break_fallthrough;
    case  3: *(By2*)(bdst -  3) = *(const By2*)(bsrc -  3); no_break_fallthrough;
    case  1: *(By1*)(bdst -  1) = *(const By1*)(bsrc -  1); no_break_fallthrough;
    case  0:  break;
    case 13: *(By8*)(bdst - 13) = *(const By8*)(bsrc - 13); no_break_fallthrough;
    case  5: *(By1*)(bdst -  5) = *(const By1*)(bsrc -  5); no_break_fallthrough;
    case  4: *(By4*)(bdst -  4) = *(const By4*)(bsrc -  4);
              break;
    case 14: *(By8*)(bdst - 14) = *(const By8*)(bsrc - 14); no_break_fallthrough;
    case  6: *(By4*)(bdst -  6) = *(const By4*)(bsrc -  6); no_break_fallthrough;
    case  2: *(By2*)(bdst -  2) = *(const By2*)(bsrc -  2);
              break;
    case 12: *(By4*)(bdst - 12) = *(const By4*)(bsrc - 12); no_break_fallthrough;
    case  8: *(By8*)(bdst -  8) = *(const By8*)(bsrc -  8);
              break;
    case 11: *(By2*)(bdst - 11) = *(const By2*)(bsrc - 11); no_break_fallthrough;
    case  9: *(By1*)(bdst -  9) = *(const By1*)(bsrc -  9);
             *(By8*)(bdst -  8) = *(const By8*)(bsrc -  8);
              break;
    case 10: *(By8*)(bdst - 10) = *(const By8*)(bsrc - 10);
             *(By2*)(bdst -  2) = *(const By2*)(bsrc -  2);
              break;
    }
    return bdst;
}

static inline //HSM_FORCE_INLINE
byte_t* small_memcpy_align_8(void* dst, const void* src, size_t len) {
    assert(len % 8 == 0);
    assert(size_t(dst) % 8 == 0);
    assert(size_t(src) % 8 == 0);
    for (size_t i = 0; i < len; i += 8) {
        *(uint64_t*)((byte_t*)dst + i) = *(uint64_t*)((byte_t*)src + i);
    }
    return (byte_t*)dst + len;
}

static inline //HSM_FORCE_INLINE
byte_t* small_memcpy_align_4(void* dst, const void* src, size_t len) {
    assert(len % 4 == 0);
    assert((size_t(dst) & 3) == 0);
    assert((size_t(src) & 3) == 0);
	typedef uint64_t  By8 TERARK_GNU_UNALIGNED;
	typedef uint32_t  By4 TERARK_GNU_UNALIGNED;
	typedef uint8_t   By1;
    while (len >= 16) {
	    __m128i m0 = _mm_loadu_si128((const __m128i*)src);
	    _mm_storeu_si128((__m128i*)dst, m0);
        len -= 16;
        src = (const By1*)src + 16;
        dst = (      By1*)dst + 16;
    }
    if (len < 8) {
        if (len)
            *(By4*)dst = *(const By4*)src;
    }
    else {
        *(By8*)dst = *(const By8*)src;
        if (12 == len)
            ((By4*)dst)[2] = ((const By4*)src)[2];
    }
    return (By1*)dst + len;
}

static inline //HSM_FORCE_INLINE
byte_t* tiny_memcpy_align_4(void* dst, const void* src, size_t len) {
    assert(len % 4 == 0);
    assert((size_t(dst) & 3) == 0);
    assert((size_t(src) & 3) == 0);
    auto Dst = (      uint32_t*)dst;
    auto Src = (const uint32_t*)src;
    while (len) {
        *Dst++ = *Src++;
        len -= 4;
    }
    return (byte_t*)Dst;
}

static inline //HSM_FORCE_INLINE
byte_t* tiny_memcpy_align_1(void* dst, const void* src, size_t len) {
    auto Dst = (      byte_t*)dst;
    auto Src = (const byte_t*)src;
    while (len) {
        *Dst++ = *Src++;
        len--;
    }
    return Dst;
}

static inline
byte_t* tiny_memset_align_1(void* dst, unsigned char val, size_t len) {
    unsigned char* bdst = (unsigned char*)dst;
    while (len) {
        *bdst++ = val;
        len--;
    }
    return bdst;
}

static inline
byte_t* tiny_memset_align_p(void* dst, unsigned char val, size_t align) {
    assert((align & (align-1)) == 0);
    unsigned char* bdst = (unsigned char*)dst;
    while (size_t(bdst) & (align-1)) {
        *bdst++ = val;
    }
    return bdst;
}

static inline
byte_t* tiny_memset_align_4(void* dst, uint32_t val, size_t len) {
    assert(len % 4 == 0);
    assert((size_t(dst) & 3) == 0);
    uint32_t* Dst = (uint32_t*)dst;
    while (len) {
        *Dst++ = val;
        len -= 4;
    }
    return (byte_t*)Dst;
}

} // namespace terark 
