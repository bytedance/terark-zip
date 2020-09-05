/* vim: set tabstop=4 : */
#ifndef __terark_stdtypes_h__
#define __terark_stdtypes_h__

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
# pragma warning(disable: 4819)
# pragma warning(disable: 4290)
# pragma warning(disable: 4267) // conversion from 'size_t' to 'uint', possible loss of data
# pragma warning(disable: 4244) // conversion from 'difference_type' to 'int', possible loss of data
#endif

#include <assert.h>
#include "config.hpp"

#include <boost/predef/other/endian.h>
#include <boost/current_function.hpp>
#include <limits.h>

#if defined(_MSC_VER)
	#if _MSC_VER >= 1800
		#include <stdint.h>
	#else
		#include <boost/cstdint.hpp>
		using boost::int8_t;
		using boost::int16_t;
		using boost::int32_t;

		using boost::uint8_t;
		using boost::uint16_t;
		using boost::uint32_t;

	  #if !defined(BOOST_NO_INT64_T)
		using boost::int64_t;
		using boost::uint64_t;
	  #endif
	#endif
#else // assume c99/gcc compatible
	#include <stdint.h>
#endif

#include <string.h> // for memcpy

namespace terark {

typedef unsigned char uint08_t;
typedef   signed char  int08_t;
typedef unsigned char   byte_t;
typedef unsigned char   byte;
typedef   signed char  sbyte_t;
typedef   signed char  sbyte;
typedef unsigned short ushort;
typedef unsigned int   uint;
typedef unsigned long  ulong;
typedef unsigned long long ullong;
typedef long long llong;

typedef size_t bm_uint_t;
static const size_t WordBits = sizeof(bm_uint_t) * 8;

#if !defined(BOOST_NO_INT64_T)
typedef uint64_t stream_position_t;
typedef int64_t  stream_offset_t;
#else
typedef uint32_t stream_position_t;
typedef int32_t  stream_offset_t;
#endif

typedef uint32_t ip_addr_t;

//! 空类，多用于参数化继承时的基类占位符
class EmptyClass{};

#define TT_PAIR(T) std::pair<T,T>

template<class SizeT, class AlignT>
inline SizeT align_up(SizeT size, AlignT align_size)
{
	size = (size + align_size - 1);
	return size - size % align_size;
}

template<class SizeT, class AlignT>
inline SizeT align_down(SizeT size, AlignT align_size)
{
	return size - size % align_size;
}

template<class SizeT, class AlignT>
inline SizeT remain_on_align(SizeT n, AlignT align)
{
/*  if (size % align == 0)
        return 0;
    else
        return align - (n % align);
*/
    return align - 1 - (n - 1) % align;
}

template<class SizeT, class AlignT>
inline SizeT pow2_align_up(SizeT size, AlignT align) {
    assert(((align-1) & align) == 0);
	return (size + align-1) & ~SizeT(align-1);
}

template<class SizeT, class AlignT>
inline SizeT pow2_align_down(SizeT size, AlignT align) {
    assert(((align-1) & align) == 0);
	return size & ~SizeT(align-1);
}

template<class T, class U>
inline T& maximize(T& x, const U& y) {
    if (x < y)
        x = y;
    return x;
}

template<class T, class U>
inline T& minimize(T& x, const U& y) {
    if (y < x)
        x = y;
    return x;
}

// StaticUintBits<255>::value = 8
// StaticUintBits<256>::value = 9
template<size_t x>
struct StaticUintBits {
	enum { value = StaticUintBits< (x >> 1) >::value + 1 };
};
template<>
struct StaticUintBits<0> { enum { value = 0 }; };

template<class IntX, class IntY>
inline IntX ceiled_div(IntX x, IntY y) { return (x + y - 1) / y; }

#define TERARK_HAS_BSET(set, subset) (((set) & (subset)) == (subset))

/*
 * iter = s.end();
 * ibeg = s.begin();
 * if (iter != ibeg) do { --iter;
 *     //
 *     // do something with iter
 *     //
 * } while (iter != ibeg);else;
 * //
 * // this 'else' is intend for use REVERSE_FOR_EACH
 * // within an if-else-while sub sentence
 *
 * // this is faster than using reverse_iterator
 *
 */
//#define REVERSE_FOR_EACH_BEGIN(iter, ibeg)  if (iter != ibeg) do { --iter
//#define REVERSE_FOR_EACH_END(iter, ibeg)    } while (iter != ibeg); else


/////////////////////////////////////////////////////////////
//
//! @note Need declare public/protected after call this macro!!
//
#define DECLARE_NONE_COPYABLE_CLASS(ThisClassName)	\
	ThisClassName(const ThisClassName&) = delete; \
	ThisClassName& operator=(const ThisClassName&) = delete;
/////////////////////////////////////////////////////////////

/// stronger than none copyable
#define DECLARE_NONE_MOVEABLE_CLASS(ThisClassName) \
    DECLARE_NONE_COPYABLE_CLASS(ThisClassName); \
	ThisClassName(ThisClassName&&) = delete; \
	ThisClassName& operator=(ThisClassName&&) = delete;


#define CURRENT_SRC_CODE_POSTION  \
	__FILE__ ":" BOOST_STRINGIZE(__LINE__) ", in function: " BOOST_CURRENT_FUNCTION

#define TERARK_DIE(fmt, ...) \
	do { \
        fprintf(stderr, "%s:%d: %s: die: " fmt " !\n", \
                __FILE__, __LINE__, BOOST_CURRENT_FUNCTION, ##__VA_ARGS__); \
        abort(); } while (0)

/// VERIFY indicate runtime assert in release build
#define TERARK_VERIFY_F_IMP(expr, fmt, ...) \
    do { if (terark_unlikely(!(expr))) { \
        fprintf(stderr, "%s:%d: %s: verify(%s) failed" fmt " !\n", \
                __FILE__, __LINE__, BOOST_CURRENT_FUNCTION, #expr, ##__VA_ARGS__); \
        abort(); }} while (0)

#define TERARK_VERIFY_F(expr, fmt, ...) \
        TERARK_VERIFY_F_IMP(expr, ": " fmt, ##__VA_ARGS__)

#if defined(_DEBUG) || defined(DEBUG) || !defined(NDEBUG)
#	define DEBUG_only(S) S
#	define DEBUG_perror		perror
#	define DEBUG_printf		printf
#	define DEBUG_fprintf	fprintf
#	define DEBUG_fflush		fflush
#	define TERARK_IF_DEBUG(Then, Else)  Then
#	define TERARK_RT_assert(exp, ExceptionT)  assert(exp)
#	define TERARK_ASSERT_F TERARK_VERIFY_F
#	define TERARK_VERIFY assert
#else
#	define DEBUG_only(S)
#	define DEBUG_perror(Msg)
#	define DEBUG_printf		1 ? (void)0 : (void)printf
#	define DEBUG_fprintf	1 ? (void)0 : (void)fprintf
#	define DEBUG_fflush(fp)
#	define TERARK_IF_DEBUG(Then, Else)  Else
#	define TERARK_RT_assert(exp, ExceptionT)  \
	if (terark_unlikely(!(exp))) { \
		terark::string_appender<> oss;\
		oss << "expression=\"" << #exp << "\", exception=\"" << #ExceptionT << "\"\n" \
			<< __FILE__ ":" BOOST_STRINGIZE(__LINE__) ", in function: " \
			<< BOOST_CURRENT_FUNCTION; \
		throw ExceptionT(oss.str().c_str()); \
	}
#	define TERARK_ASSERT_F(...)
#	define TERARK_VERIFY(expr) TERARK_VERIFY_F_IMP(expr, "")
#endif

#define TERARK_ASSERT_LT(x,y) TERARK_ASSERT_F(x <  y, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_ASSERT_GT(x,y) TERARK_ASSERT_F(x >  y, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_ASSERT_LE(x,y) TERARK_ASSERT_F(x <= y, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_ASSERT_GE(x,y) TERARK_ASSERT_F(x >= y, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_ASSERT_EQ(x,y) TERARK_ASSERT_F(x == y, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_ASSERT_NE(x,y) TERARK_ASSERT_F(x != y, "%lld %lld", (long long)(x), (long long)(y))

// _EZ: Equal To Zero
#define TERARK_ASSERT_EZ(x) TERARK_ASSERT_F(x == 0, "%lld", (long long)(x))

// _AL: Align, _NA: Not Align
#define TERARK_ASSERT_AL(x,a) TERARK_ASSERT_F((x) % (a) == 0, "%lld %% %lld = %lld", (long long)(x), (long long)(a), (long long)((x) % (a)))
#define TERARK_ASSERT_NA(x,a) TERARK_ASSERT_F((x) % (a) != 0, x)

#define TERARK_VERIFY_LT(x,y) TERARK_VERIFY_F(x <  y, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_VERIFY_GT(x,y) TERARK_VERIFY_F(x >  y, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_VERIFY_LE(x,y) TERARK_VERIFY_F(x <= y, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_VERIFY_GE(x,y) TERARK_VERIFY_F(x >= y, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_VERIFY_EQ(x,y) TERARK_VERIFY_F(x == y, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_VERIFY_NE(x,y) TERARK_VERIFY_F(x != y, "%lld %lld", (long long)(x), (long long)(y))

// _EZ: Equal To Zero
#define TERARK_VERIFY_EZ(x) TERARK_VERIFY_F(x == 0, "%lld", (long long)(x))

// _AL: Align, _NA: Not Align
#define TERARK_VERIFY_AL(x,a) TERARK_VERIFY_F((x) % (a) == 0, "%lld %% %lld = %lld", (long long)(x), (long long)(a), (long long)((x) % (a)))
#define TERARK_VERIFY_NA(x,a) TERARK_VERIFY_F((x) % (a) != 0, "%lld", (long long)(x))

} // namespace terark

template<class T>
inline T aligned_load(const void* p) {
   	return *reinterpret_cast<const T*>(p);
}
template<class T>
inline T unaligned_load(const void* p) {
   	T x;
   	memcpy(&x, p, sizeof(T));
   	return x;
}
template<class T>
inline T aligned_load(const void* p, size_t i) {
   	return reinterpret_cast<const T*>(p)[i];
}
template<class T>
inline T unaligned_load(const void* p, size_t i) {
   	T x;
   	memcpy(&x, (const char*)(p) + sizeof(T) * i, sizeof(T));
   	return x;
}

template<class T>
inline void   aligned_save(void* p,T x) { *reinterpret_cast<T*>(p) = x; }
template<class T>
inline void unaligned_save(void* p,T x) { memcpy(p, &x, sizeof(T)); }

template<class T>
inline void aligned_save(void* p, size_t i, T val) {
   	reinterpret_cast<T*>(p)[i] = val;
}
template<class T>
inline void unaligned_save(void* p, size_t i, T val) {
   	memcpy((char*)(p) + sizeof(T) * i, &val, sizeof(T));
}

#endif // __terark_stdtypes_h__
