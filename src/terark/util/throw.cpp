#include "throw.hpp"
#include "autofree.hpp"
#include <stdio.h>
#include <errno.h>
#include <stdarg.h>

namespace terark {

TERARK_DLL_EXPORT
std::string ExceptionFormatString(const char* format, ...) {
#ifdef _MSC_VER
    std::string buf(16*1024, '\0');
    va_list ap;
    va_start(ap, format);
	int len = _vsnprintf(&buf[0], buf.size(), format, ap);
    va_end(ap);
    buf.resize(len);
    buf.shrink_to_fit();
	fprintf(stderr, "%s\n", buf.c_str());
	return buf;
#else
	terark::AutoFree<char> buf;
    va_list ap;
    va_start(ap, format);
	int len = vasprintf(&buf.p, format, ap);
    va_end(ap);
	fprintf(stderr, "%s\n", buf.p);
	return std::string(buf.p, len);
#endif
}

}
