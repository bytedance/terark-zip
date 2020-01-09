#pragma once

#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>

namespace terark {

class TERARK_DLL_EXPORT TempFileDeleteOnClose {
public:
  std::string path;
  FileStream fp;
  NativeDataOutput<OutputBuffer> writer;

  ~TempFileDeleteOnClose();
  void open_temp();
  void open();
  void dopen(int fd);
  void close();
  void complete_write();
};

struct TERARK_DLL_EXPORT FilePair {
  TempFileDeleteOnClose key;
  TempFileDeleteOnClose value;
  bool isFullValue = true;
};


class TERARK_DLL_EXPORT AutoDeleteFile {
public:
  std::string fpath;
  operator fstring() const { return fpath; }
  void Delete();
  ~AutoDeleteFile();
};

}
