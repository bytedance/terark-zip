#include <algorithm>
#include <iostream>
#include <cassert>
#include <string>
#include <string.h>
#include <fstream>
#include <memory>

#include <inttypes.h>

#include "terark/sdk/nlt.hpp"
#include "terark/sdk/utils.hpp"

#define GROUP_SIZE 8

void print_chars(const char* p, int start, int length) {
  for(int i = start; i < start + length; ++i) {
    printf("%c", p[i]);
  }
}

void print_int64(const char* p, int start) {
  printf("%llu", *(uint64_t*)(p + start));
}

void write_key(bool is_binary, const std::string& key, std::ofstream& out) {
  uint32_t len = key.size();
  if(is_binary) {
    out.write((char*)&len, 4);
    out.write(key.data(), len);

    // printf("len = %d, ", len);
    // print_chars(key.data(), 0, len);
    // printf(":");
    // print_int64(key.data(), len + 8);
    // printf("\n");
  } else {
    out.write(key.data(), len);
    out.write("\n", 1);
  }
}

/**
 * binary format:
 *  [len1][key1][len2][key2]...
 *
 * line format:
 *  key1
 *  key2
 *  key3
 *  ...
 *
 */
int main(int argc, char** argv) {
  bool is_encode = true;
  bool is_binary = true;

  if(argc < 5) {
    std::cout << "usage: ./nlt_build [encode|decode] [binary|line] [input_file|input_prefix] output_file" << std::endl;
    return 1;
  } if(memcmp(argv[1], "encode", 6) == 0) {
    is_encode = true;
  } else if(memcmp(argv[1], "decode", 6) == 0) {
    is_encode = false;
  } else {
    std::cout << "usage: ./nlt_build [encode|decode] [binary|line] [input_file|input_prefix] output_file" << std::endl;
    return 1;
  }

  if(memcmp(argv[2], "binary", 6) == 0) {
    is_binary = true;
  }else if (memcmp(argv[2], "line", 4) == 0) {
    is_binary = false;
  }else {
    std::cout << "usage: ./nlt_build [encode|decode] [binary|line] [input_file|input_prefix] output_file" << std::endl;
    return 2;
  }

  std::string ifile = std::string(argv[3]);
  std::string ofile = std::string(argv[4]);

  if(is_encode) {
    printf("encoding...\n");
    auto nlt_write = new terark::NLT<>(true, true);
    nlt_write->build(ifile, ofile, 50000000, is_binary);
    delete nlt_write;
  } else {
    std::ofstream out(ofile, std::ios::binary | std::ios::out);
    printf("decoding...\n");
    // input_file for decoding means its prefix
    // e.g. input_prefix : abc.nlt will be treated as abc.nlt-1, abc.nlt-2 ...
    // the sequence should be continious from 1 to N
    int i = 0;
    auto fname = ifile;

    while(true) {
      if(i > 0) {
        fname = ifile + "-" + std::to_string(i);
        if(!terark::file_exist(fname.data())) {
          printf("end of all nlt files, exit!\n");
          break;
        }
      } else {
        if(!terark::file_exist(fname.data())) {
          printf("first nlt file doesn't exist, skip, fname = %s\n", fname.data());
          ++i;
          continue;
        }
      }
      ++i;

      printf("processing nlt file: %s\n", fname.data());
      auto nlt_read = std::make_shared<terark::NLT<>>(fname);
      if(nlt_read->seek_begin()) {
        std::string key;
        nlt_read->get_key(key);
        write_key(is_binary, key, out);

        while(nlt_read->next()){
          nlt_read->get_key(key);
          write_key(is_binary, key, out);
        }
      }else{
        printf("seek begin error, exit!\n");
        break;
      }
    }
    out.close();
  }
  return 0;
}
