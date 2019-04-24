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

} // namespace terark
