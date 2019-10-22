#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/util/mmap.hpp>

namespace terark {

class TempFileDeleteOnClose {
public:
  std::string path;
  FileStream fp;
  NativeDataOutput<OutputBuffer> writer;

  ~TempFileDeleteOnClose() {
    if (fp)
      this->close();
  }

  void open_temp(){
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

  void open(){
    fp.open(path.c_str(), "wb+");
    fp.disbuf();
    writer.attach(&fp);
  }

  void dopen(int fd) {
    fp.dopen(fd, "wb+");
    fp.disbuf();
    writer.attach(&fp);
  }

  void close() {
    assert(nullptr != fp);
    writer.resetbuf();
    fp.close();
    ::remove(path.c_str());
  }

  void complete_write() {
    writer.flush_buffer();
    fp.rewind();
  }
};

struct FilePair {
  TempFileDeleteOnClose key;
  TempFileDeleteOnClose value;
  bool isFullValue = true;
};


class AutoDeleteFile {
public:
  std::string fpath;
  operator fstring() const { return fpath; }
  void Delete() {
    if (!fpath.empty()) {
      ::remove(fpath.c_str());
      fpath.clear();
    }
  }
  ~AutoDeleteFile(){
    if (!fpath.empty()) {
      ::remove(fpath.c_str());
    }
  }
};

}
