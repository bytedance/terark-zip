#pragma once

#include <fcntl.h>
#include <fstream>
#include <string>

#define TIME_START(VAR_) VAR_ = std::chrono::high_resolution_clock::now()
#define TIME_END(VAR_) VAR_ = std::chrono::high_resolution_clock::now()

#define PRINT_TIME(START, END)                                          \
  auto duration__ = END - START;                                        \
  auto ms__ =                                                           \
      std::chrono::duration_cast<std::chrono::milliseconds>(duration__) \
          .count();                                                     \
  std::cout << name " took " << ms__ << " ms" << std::endl

namespace terark {

inline bool file_exist(const char *fname) {
  int fd = ::open(fname, O_RDONLY);
  return fd > 0;
}

/**
 * Get line from ifstream by last line breaker delim and emit the last delim.
 * e.g.
 *  get line from : [abcd\n\nefgh] will have [abcd\n] and leave [efgh] in the
 * stream.
 *
 */
// inline int getline(ifstream& ifile, std::string line) {
// TODO
//  return 1;
//}

template <typename F>
int parse_lines(const char *fname, const F &&callback) {
  size_t cnt = 0;
  std::ifstream infile(fname);
  if (infile.fail()) {
    std::cout << "source file doesn't exist, exit ! file name = "
              << std::string(fname) << std::endl;
    exit(0);
  }
  std::string line;
  while (std::getline(infile, line)) {
    callback(line);
    ++cnt;
  }
  std::cout << "file parse finished, total rows = " << cnt
            << ", file name = " << fname << std::endl;
  return cnt;
}

}  // namespace terark