#include <atomic>
#include <cstdio>
#include <iostream>
#include <random>
#include <thread>

#include "gtest/gtest.h"

#include "../utils.hpp"
#include "zbs.hpp"
#include "zbs_dict_zip.hpp"
#include "zbs_entropy.hpp"
#include "zbs_mixed_len.hpp"

#include <terark/zbs/mixed_len_blob_store.hpp>

// inline void print_bytes(const std::string &str) {
//   const char *c = str.c_str();
//   for (int i = 0; i < str.size(); ++i) {
//     printf("%u ", (unsigned char)c[i]);
//   }
//   printf("\n");
// }

// TEST(ZBS_TEST, TEST_GEN_STR) {
//   print_bytes(gen_str_with_padding<10>(5));
//   print_bytes(gen_str_with_padding<10>(0));
//   print_bytes(gen_str_with_padding<10>(1));
//   print_bytes(gen_str_with_padding<10>(10));
// }

/**
 * test using entroy blob store
 */
TEST(ZBS_TEST, BASIC_ENTROPY_ZBS) {
  std::cout << "zbs basic test" << std::endl;
  fstring raw_fname, zbs_file;

  if (terark::file_exist("/Users/guokuankuan/Downloads/15g_head.sql")) {
    raw_fname = "/Users/guokuankuan/Downloads/15g_head.sql";
    zbs_file = "/Users/guokuankuan/Downloads/15g_head.zbs";
  } else if (terark::file_exist("/data00/bmq_data/raw.pb")) {
    raw_fname = "/data00/bmq_data/raw.pb";
    zbs_file = "/data00/bmq_data/raw.pb.zbs";
  } else {
    std::cout << "test file doesn't exist, skip!" << std::endl;
    return;
  }

  //    ZBSDictZip zbs;
  ZBSEntropy zbs;
  int record_size = zbs.compress(
      raw_fname, zbs_file, terark::ZBS_INPUT_FILE_TYPE::LINE_BASED_RECORDS);
  std::cout << "total record size = " << record_size << std::endl;

  // must load zbs file first before read
  ZBSEntropy zbs2;
  zbs2.load_zbs(zbs_file);

  valvec<byte_t> record;
  for (int i = 0; i < 10; ++i) {
    zbs2.get(i, &record);
    std::cout << i << " : " << record.data() << std::endl;
  }
}

/**
 * test using dict zip blob store
 */
TEST(ZBS_TEST, BASIC_DICT_ZIP_ZBS) {
  std::cout << "zbs basic test" << std::endl;
  fstring raw_fname, zbs_file;

  if (terark::file_exist("/Users/guokuankuan/Downloads/15g_head.sql")) {
    raw_fname = "/Users/guokuankuan/Downloads/15g_head.sql";
    zbs_file = "/Users/guokuankuan/Downloads/15g_head.zbs";
  } else if (terark::file_exist("/data00/bmq_data/raw.pb")) {
    raw_fname = "/data00/bmq_data/raw.pb";
    zbs_file = "/data00/bmq_data/raw.pb.zbs";
  } else {
    std::cout << "test file doesn't exist, skip!" << std::endl;
    return;
  }

  ZBSDictZip zbs(3, true);
  int record_size = zbs.compress(
      raw_fname, zbs_file, terark::ZBS_INPUT_FILE_TYPE::LINE_BASED_RECORDS);
  std::cout << "total record size = " << record_size << std::endl;

  // must load zbs file first before read
  ZBSDictZip zbs2(3, true);
  zbs2.load_zbs(zbs_file);

  valvec<byte_t> record;
  for (int i = 0; i < 10; ++i) {
    zbs2.get(i, &record);
    std::cout << i << " : " << record.data() << std::endl;
  }
}

/**
 * test using splittable input source & entropy blob store
 */
TEST(ZBS_TEST, SPLIT_AND_COMPRESS) {
  std::cout << "split and compress test" << std::endl;
  fstring raw_fname, zbs_file;
  if (terark::file_exist("/Users/guokuankuan/Downloads/15g_head.sql")) {
    raw_fname = "/Users/guokuankuan/Downloads/15g_head.sql";
    zbs_file = "/Users/guokuankuan/Downloads/15g_head_split.sql.zbs";
  } else {
    std::cout << "split and compress test skip!" << std::endl;
    return;
  }

  ZBSDictZip zbs(3, false);
  zbs.compress(
      raw_fname, zbs_file,
      [](const MmapWholeFile& fmmap,
         const std::function<void(const fstring record)>& record_parser) {
        ZBS::split_binary(fmmap, 10, record_parser);
      });

  ZBSDictZip zbs2(3, false);
  zbs2.load_zbs(zbs_file);

  valvec<byte_t> record;
  for (int i = 0; i < 10; ++i) {
    zbs2.get(i, &record);
    std::cout << i << " : " << record.data() << std::endl;
  }
}

inline std::string gen_str_with_padding(int fill, char* buffer, int buf_size) {
  memset(buffer, 0, buf_size);
  for (int i = 0; i < fill; ++i) {
    buffer[i] = rand() % 255;
  }
  return std::string(buffer, buf_size);
}

TEST(ZBS_TEST, MIXED_LEN_BLOB_STORE) {
  const int fixed_len = (16 << 10) + 7;  // ~ 16KB
  const int total_records = 1 << 22;     // 4 million
  std::string nlt_fname = "mixed_len_blob_store.test.zbs";

  terark::MixedLenBlobStore::MyBuilder builder(fixed_len, 0 /*varKeb*/,
                                               0 /*varLenCnt*/, nlt_fname, 0 /*offset*/,
                                               2 /*checksumLevel*/, 0 /*checksumType*/);

  std::random_device random_device;
  std::mt19937 gen(random_device());
  std::uniform_int_distribution<int> distribution_0_16K(1, fixed_len - 7);
  std::vector<std::string> records;
  records.reserve(total_records);

  std::cout << "start record generation..." << std::endl;

  // prepare a repeatable use buffer
  char* ramdom_buffer = (char*)malloc(fixed_len);

  for (int i = 0; i < total_records; ++i) {
    int fill = distribution_0_16K(gen) + 7;  // 7 bytes seqno before record value.
    records.emplace_back(gen_str_with_padding(fill, ramdom_buffer, fixed_len));
    if (i % (total_records / 10) == 0) {
      std::cout << "finish generate " << i << " records" << std::endl;
    }
  }

  free(ramdom_buffer);

  std::cout << "finish record generation, add records..." << std::endl;

  // take all cpu resource
  std::vector<std::thread> workers;
  std::atomic<uint64_t> tick = {0};
  std::atomic_bool shutdown = {false};
  // start 50 busy waiting threads
  for (int i = 0; i < 50; ++i) {
    workers.emplace_back([i, &shutdown, &tick]() {
      std::cout << "start thread " << i + 1 << std::endl;
      while (!shutdown) {
        if (i % 2 == 0) {
          tick++;
        } else {
          tick--;
        }
      }
    });
  }

  for (size_t i = 0; i < total_records; ++i) {
    builder.addRecord(records[i]);
  }
  builder.finish();

  std::unique_ptr<terark::AbstractBlobStore> store;
  store.reset(terark::AbstractBlobStore::load_from_mmap(nlt_fname, false));

  for (int i = 0; i < total_records; ++i) {
    auto item = store->get_record(i);
    ASSERT_EQ(item.size(), fixed_len);
    ASSERT_EQ(memcmp(item.data(), records[i].data(), fixed_len), 0);
    if (i % (total_records / 10) == 0) {
      std::cout << "verified " << i << " records" << std::endl;
    }
  }

  shutdown = true;

  for (auto& worker : workers) {
    worker.join();
  }

  // Read Data and Validate
}
