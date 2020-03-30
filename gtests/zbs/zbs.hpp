#pragma once

#include <stdint.h>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>

#include <terark/fsa/fsa.hpp>
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/mmap.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/zbs/dict_zip_blob_store.hpp>
#include <terark/zbs/entropy_zip_blob_store.hpp>

using namespace terark;

//
//  ZBS
//    |__ ZBSDictZip
//    |__ ZBSEntropy
//    |__ ZBSMixedLen
//
namespace terark {
#define isNewLine(c) ('\n' == c || '\r' == c)

enum ZBS_INPUT_FILE_TYPE {
  LINE_BASED_RECORDS = 0,  // records are seperated by line seperators.
  FORMATTED_BINARY = 1,    // [len, data, len, data, len, data ...]
  RAW_BINARY = 2  // big blob data file, should be splitted before compress
};

class ZBS {
 protected:
  static void _extractRecords(
      const MmapWholeFile &mmap_raw_file, const ZBS_INPUT_FILE_TYPE file_type,
      const std::function<void(const fstring line)> &parse_record) {
    auto beg = (const char *)mmap_raw_file.base;
    auto end = beg + mmap_raw_file.size;
    auto line_end = beg;
    size_t i = 0;
    while (beg != end) {
      // parse lines by different input format
      switch (file_type) {
        case LINE_BASED_RECORDS:
          // use line breaker to detech line_ends.
          while (line_end < end && !isNewLine(*line_end)) ++line_end;
          while (line_end < end && isNewLine(*line_end)) ++line_end;
          break;
        case FORMATTED_BINARY: {
          // binary layout: [length][data][length][data]...
          uint32_t data_len = *(uint32_t *)beg;
          beg += 4;
          line_end += (4 + data_len);
          break;
        }
        default:
          // something goes wrong here!
          std::cout << "wrong data format!" << std::endl;
          return;
      }

      fstring line(beg, line_end);
      parse_record(line);
      beg = line_end;

      if (i % (1000 * 1000) == 0) {
        std::cout << "records processed : " << i << " records..." << std::endl;
      }
      ++i;
    }
  }

 public:
  /**
   * A demostration for self-defined input parser.
   * @param F: const std::function<void(const fstring record)> &record_parser
   */
  template <typename F>
  static void split_binary(const MmapWholeFile &raw_mmap, int split_size,
                           F &&record_parser) {
    auto beg = (const char *)raw_mmap.base;
    auto end = beg + raw_mmap.size;
    auto record_end = beg;
    size_t i = 0;

    while (beg != end) {
      // parse records by fixed length
      if (end - beg >= split_size) {
        record_end += split_size;
      } else {
        record_end = end;
      }

      fstring line(beg, record_end);
      record_parser(line);
      beg = record_end;

      if (i % (1000 * 1000) == 0) {
        std::cout << "processing : " << i << " records..." << std::endl;
      }
      ++i;
    }

    std::cout << "total records: " << i << std::endl;
  }
};

}  // namespace terark