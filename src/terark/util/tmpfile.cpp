// created by leipeng at 2020-01-09 10:32

#include "tmpfile.hpp"
#if _MSC_VER
  #include <io.h>
#endif

namespace terark {

  TempFileDeleteOnClose::~TempFileDeleteOnClose() {
    if (fp)
        this->close();
  }

  void TempFileDeleteOnClose::open_temp() {
    if (!fstring(path).endsWith("XXXXXX")) {
      THROW_STD(invalid_argument,
                "ERROR: path = \"%s\", must ends with \"XXXXXX\"", path.c_str());
    }
  #if _MSC_VER
    if (int err = _mktemp_s(&path[0], path.size() + 1)) {
      THROW_STD(invalid_argument, "ERROR: _mktemp_s(%s) = %s"
          , path.c_str(), strerror(err));
    }
    this->open();
  #else
    int fd = mkstemp(&path[0]);
    if (fd < 0) {
      int err = errno;
      THROW_STD(invalid_argument, "ERROR: mkstemp(%s) = %s", path.c_str(), strerror(err));
    }
    this->dopen(fd);
  #endif
  }

  void TempFileDeleteOnClose::open(){
    fp.open(path.c_str(), "wb+");
    fp.disbuf();
    writer.attach(&fp);
  }

  void TempFileDeleteOnClose::dopen(int fd) {
    fp.dopen(fd, "wb+");
    fp.disbuf();
    writer.attach(&fp);
  }

  void TempFileDeleteOnClose::close() {
    assert(nullptr != fp);
    writer.resetbuf();
    fp.close();
    ::remove(path.c_str());
  }

  void TempFileDeleteOnClose::complete_write() {
    writer.flush_buffer();
    fp.rewind();
  }

  void AutoDeleteFile::Delete() {
    if (!fpath.empty()) {
      ::remove(fpath.c_str());
      fpath.clear();
    }
  }
  AutoDeleteFile::~AutoDeleteFile(){
    if (!fpath.empty()) {
      ::remove(fpath.c_str());
    }
  }

} // namespace terark
