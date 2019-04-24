#include "checksum_exception.hpp"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif  // __STDC_FORMAT_MACROS

#include <inttypes.h>

namespace terark {

BadChecksumException::~BadChecksumException() {}

static std::string ChecksumErrMsg(fstring msg, uint64_t Old, uint64_t New) {
  char buf[72];
  std::string res;
  res.reserve(msg.size() + 64);
  res.append(msg.data(), msg.size());
  res.append(buf, sprintf(buf, ": Old = 0x%16" PRIX64 " , New = 0x%16" PRIX64, Old, New));
  return res;
}

BadChecksumException::
BadChecksumException(fstring msg, uint64_t Old, uint64_t New)
  : super(ChecksumErrMsg(msg, Old, New)), m_old(Old), m_new(New) {}

} // terark

