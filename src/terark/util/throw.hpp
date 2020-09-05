#pragma once
#include <terark/config.hpp>
#include <boost/current_function.hpp>
#include <string>
#include <stdexcept>
#include <cerrno>

namespace terark {

TERARK_DLL_EXPORT
std::string ExceptionFormatString(const char* format, ...)
#ifdef __GNUC__
	__attribute__ ((__format__ (__printf__, 1, 2)))
#endif
;

#define ExceptionMessage(fmt, ...) \
    terark::ExceptionFormatString("%s:%d: %s: errno=%d : " fmt, \
        __FILE__, __LINE__, BOOST_CURRENT_FUNCTION, errno, ##__VA_ARGS__)

#define TERARK_THROW(Except, fmt, ...) \
    throw Except(ExceptionMessage(fmt, ##__VA_ARGS__))

#define THROW_STD(Except, fmt, ...) \
	TERARK_THROW(std::Except, fmt, ##__VA_ARGS__)

#define TERARK_EXPECT_F(expr, Except, fmt, ...) \
  do { \
    if (!(expr))  \
      throw Except(terark::ExceptionFormatString( \
        "%s:%d: %s: expect(%s) failed: " fmt, \
        __FILE__, __LINE__, BOOST_CURRENT_FUNCTION, #expr, ##__VA_ARGS__)); \
  } while (0)

#define TERARK_EXPECT_LT(x,y,e) TERARK_EXPECT_F(x <  y, e, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_EXPECT_GT(x,y,e) TERARK_EXPECT_F(x >  y, e, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_EXPECT_LE(x,y,e) TERARK_EXPECT_F(x <= y, e, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_EXPECT_GE(x,y,e) TERARK_EXPECT_F(x >= y, e, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_EXPECT_EQ(x,y,e) TERARK_EXPECT_F(x == y, e, "%lld %lld", (long long)(x), (long long)(y))
#define TERARK_EXPECT_NE(x,y,e) TERARK_EXPECT_F(x != y, e, "%lld %lld", (long long)(x), (long long)(y))

// _EZ: Equal To Zero
#define TERARK_EXPECT_EZ(x,e) TERARK_EXPECT_F(x == 0, e, "%lld", (long long)(x))

// _AL: Align, _NA: Not Align
#define TERARK_EXPECT_AL(x,a,e) TERARK_EXPECT_F((x) % (a) == 0, e, "%lld %% %lld = %lld", (long long)(x), (long long)(a), (long long)((x) % (a)))
#define TERARK_EXPECT_NA(x,a,e) TERARK_EXPECT_F((x) % (a) != 0, e, "%lld", (long long)(x))


} // namespace terark
