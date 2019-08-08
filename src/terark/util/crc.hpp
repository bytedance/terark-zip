#ifndef __terark_util_crc_hpp__
#define __terark_util_crc_hpp__

#include <terark/util/checksum_exception.hpp>

namespace terark {

TERARK_DLL_EXPORT
uint32_t Crc32c_update(uint32_t inCrc32, const void *buf, size_t bufLen);

TERARK_DLL_EXPORT
uint16_t Crc16c_update(uint16_t inCrc16, const void *buf, size_t bufLen);

class TERARK_DLL_EXPORT BadCrc32cException : public BadChecksumException {
public:
	BadCrc32cException(fstring msg, uint32_t Old, uint32_t New)
		: BadChecksumException(msg, Old, New) {}
	~BadCrc32cException();
};

class TERARK_DLL_EXPORT BadCrc16cException : public BadChecksumException {
public:
    BadCrc16cException(fstring msg, uint16_t Old, uint16_t New)
            : BadChecksumException(msg, Old, New) {}
    ~BadCrc16cException();
};

} // terark

#endif // __terark_util_crc_hpp__

