#ifndef TERARK_TOOLS_NLT_HPP
#define TERARK_TOOLS_NLT_HPP

#include <stdio.h>
#include <cassert>
#include <iostream>

#include "terark/fsa/nest_louds_trie.hpp"
#include "terark/fsa/nest_trie_dawg.hpp"
#include "terark/util/autoclose.hpp"
#include "terark/util/linebuf.hpp"
#include "terark/util/mmap.hpp"
#include "terark/util/stat.hpp"

#define isNewLine(c) ('\n' == c || '\r' == c)

namespace terark {
/**
 * @tparam NestLoudsTrieDAWG
 *          NestLoudsTrieDAWG_Mixed_XL_256      <-- default
 *
 *
 * @tparam StrVecType
 *
 *          SortableStrVec                      <-- default
 *          SortedStrVec
 *          ZoSortedStrVec
 *          FixedLenStrVec
 *
 * +--------------------------------------------------------+
 * |              |Memory Usage|Var Key Len|Can Be UnSorted?|
 * |SortableStrVec|    High    |    Yes    |     Yes        |
 * |  SortedStrVec|   Medium   |    Yes    |!Must be Sorted!|
 * |ZoSortedStrVec|    Low     |    Yes    |!Must be Sorted!|
 * |FixedLenStrVec|    Lowest  |    No     |     Yes        |
 * +--------------------------------------------------------+
 * ZoSortedStrVec is slower than SortedStrVec(20% ~ 40% slower).
 * When using ZoSortedStrVec, you should also use a tmp dir to reduce memory
 * usage
 */
template <typename NestLoudsTrieDAWG = NestLoudsTrieDAWG_Mixed_XL_256_32_FL,
          typename StrVecType = SortableStrVec>
class NLT {
 private:
  NestLoudsTrieConfig conf;
  bool use_fast_label = false;
  std::unique_ptr<NestLoudsTrieDAWG> trie;
  std::unique_ptr<ADFA_LexIterator> iter;

  static bool checkSortOrder(fstring curr) {
    static valvec<byte_t> prev;
    static size_t nth = 0;
    nth++;
    int cmp = fstring_func::compare3()(prev, curr);
    if (cmp > 0) {
      fprintf(
          stderr,
          "ERROR: option(-s) is on but nth = %zd (base 1) is less than prev\n",
          nth);
      exit(1);
    } else if (cmp < 0) {  // prev < curr
      prev.assign(curr);
      return true;
    }
    return false;
  }

 public:
  /**
   * @param input_ascending is input keys are sorted by input_ascending order.
   * @param use_fast_label use fast label will greatly improve performance but
   * slightly reduce compression
   */
  NLT(bool input_ascending, bool use_fast_label) {
    this->conf.isInputSorted = input_ascending;
    this->use_fast_label = use_fast_label;
  }

  NLT(const std::string &nlt_file) {
    BaseDFA *dfa = BaseDFA::load_mmap(nlt_file, false);
    trie.reset(dynamic_cast<NestLoudsTrieDAWG *>(dfa));
    iter.reset(trie->adfa_make_iter());
  }

  ~NLT() {}

  /**
   * Build nlt by segments, will generate a list of nlt files
   */
  void build(const std::string &input_fname, const std::string &nlt_fname,
             bool is_binary = false) {
    build(input_fname, nlt_fname, 0, is_binary);
  }

  // batch_size = 0 means no limited, all input data build in the same time.
  void build(const std::string &input_fname, const std::string &nlt_fname,
             uint32_t batch_size, bool is_binary = false) {
    StrVecType strVec;
    MmapWholeFile fmmap(input_fname);

    // 1. Optional : Stat file size and init key container
    Auto_fclose fp;
    fp = ::fopen(input_fname.data(), "r");
    assert(fp != nullptr);
    {
      struct ll_stat st {};
      int err = ::ll_fstat(fileno(fp), &st);
      if (err) {
        fprintf(stderr, "ERROR: fstat failed = %s\n", strerror(errno));
      } else if (S_ISREG(st.st_mode)) {  // OK, get file size
        // init key container strVec.
        size_t assume_avg_len = 40;
        strVec.reserve(st.st_size / assume_avg_len, st.st_size);
      }
    }

    // 2. Read all keys into strVec
    auto beg = (char *)fmmap.base;
    auto end = beg + fmmap.size;
    auto line_end = beg;

    uint32_t cnt = 0;
    uint32_t segment = 1;
    while (beg != end) {
      // if input is not binary, use line breaker to detech line_end.
      if (!is_binary) {
        while (line_end < end && !isNewLine(*line_end)) ++line_end;
        while (line_end < end && isNewLine(*line_end)) ++line_end;
      } else {
        // binary layout: [length][data][length][data]...
        uint32_t data_len = *(uint32_t *)beg;
        // printf("data length : %d\n", data_len);
        beg += 4;
        line_end += (4 + data_len);
      }
      fstring line(beg, line_end);
      line.chomp();

      if (!conf.isInputSorted || checkSortOrder(line)) {
        strVec.push_back(line);
        ++cnt;
        // 3.1 each batch write to a new nlt file
        if (batch_size > 0 && cnt % batch_size == 0) {
          std::cout << "write segment batch, num = " << segment << std::endl;
          strVec.finish();
          trie.reset(new NestLoudsTrieDAWG());
          trie->build_from(strVec, conf);
          trie->save_mmap(nlt_fname + "-" + std::to_string(segment));
          ++segment;
        }
      }
      beg = line_end;
    }
    // add a test sample
    // uint64_t i = 4;
    // std::string a = std::string((char*)&i, 8);
    // std::string tmp = std::string("fghijklm\ton124", 14) + a;
    // strVec.push_back(tmp);
    // printf("%s\n", tmp.data());
    // end test sample

    strVec.finish();
    // test start
    // printf("--------before insert test code-----------\n");
    // for(int m = 0; m < strVec.size(); ++m){
    //  std::string key = strVec[m].str();
    //  __print_char_and_int64(key, key.size() - 8);
    // }
    // printf("--------after insert test code-----------\n");
    // test end

    std::cout << "input key size = " << cnt << std::endl;

    // 3.2. Build NestLoudsTrie and save file
    std::cout << "write last batch, num = " << segment << std::endl;
    trie.reset(new NestLoudsTrieDAWG());
    trie->build_from(strVec, conf);
    trie->save_mmap(nlt_fname + "-" + std::to_string(segment));
    // for read
    std::cout << "note: only last batch is loaded for reading" << std::endl;
    iter.reset(trie->adfa_make_iter());
    // test retrive
    // __print_all();
  }

  /**
   * -1 : not found
   *
   * @param key
   * @return
   */
  size_t lookup(const std::string &key) { return trie->get_dawg()->index(key); }

  bool seek_lower_bound(const std::string &key) {
    return iter->seek_lower_bound(key);
  }

  void get_key(std::string &key) { key = iter->word().str(); }

  bool next() { return iter->incr(); }

  bool seek_begin() { return iter->seek_begin(); }

 private:
  // ~~~~~~~~~~~ for tests only, don't use ~~~~~~~~~~~~~~~
  void __print_char_and_int64(const std::string &key, int prefix_len) {
    printf("key size : %d :", key.size());
    for (int i = 0; i < prefix_len; ++i) {
      printf("%c", key.data()[i]);
    }
    printf(":");
    for (int i = prefix_len; i < prefix_len + 8; ++i) {
      printf("%d", key.data()[i]);
    }
    printf("\n");
  }

  void __print_all() {
    printf("print all:\n");
    iter->seek_begin();
    std::string key;
    get_key(key);
    __print_char_and_int64(key, key.size());
    while (iter->incr()) {
      get_key(key);
      __print_char_and_int64(key, key.size());
    }
  }
};
}  // namespace terark
#endif
