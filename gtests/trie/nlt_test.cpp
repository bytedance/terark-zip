#include <gtest/gtest.h>
#include <getopt.h>
#include <random>

#include "terark/fsa/nest_louds_trie.hpp"
#include "terark/fsa/nest_trie_dawg.hpp"
#include "terark/util/autoclose.hpp"
#include "terark/util/stat.hpp"
#include "terark/util/linebuf.hpp"
#include "terark/util/mmap.hpp"

namespace terark {

  template<typename T>
  void load_keys(T& t) {
    t.push_back("aaa");
    t.push_back("bbb");
    t.push_back("cccc");
    t.push_back("dd");
    t.push_back("df");
    t.push_back("eee");
  }

  TEST(NLT_TEST, BUILD_SAVE_LOAD_SEARCH) {
    // define NLTDawg type
    typedef NestLoudsTrieDAWG_Mixed_XL_256_32_FL NestLoudsTrieDAWG;

    // define input vector type
		/* +--------------------------------------------------------+
     * |              |Memory Usage|Var Key Len|Can Be UnSorted?|
     * |SortableStrVec|    High    |    Yes    |     Yes        |
     * |  SortedStrVec|   Medium   |    Yes    |!Must be Sorted!|
     * |ZoSortedStrVec|    Low     |    Yes    |!Must be Sorted!|
     * |FixedLenStrVec|    Lowest  |    No     |     Yes        |
     * +--------------------------------------------------------+
     * ZoSortedStrVec is slower than SortedStrVec(20% ~ 40% slower).
     * When using ZoSortedStrVec, you should also use a tmp dir to reduce memory usage
		 */
    typedef SortableStrVec StrVecType;

    // prepare trie config
    NestLoudsTrieConfig conf;
    conf.isInputSorted = true; // ascending sorted

    // prepare input vector
    StrVecType records;
    load_keys(records);
    records.finish(); // shrink to fit
    ASSERT_TRUE(records[0] == "aaa");
    ASSERT_TRUE(records[2] == "cccc");

    // prepare adfa trie
    std::unique_ptr<NestLoudsTrieDAWG> trie(new NestLoudsTrieDAWG());
    trie->build_from(records, conf);
    // optional: 
    // trie->save_mmap("/tmp/test.nlt");

    // read from adfa trie
    auto iter = trie->adfa_make_iter();
    auto found = iter->seek_lower_bound("bbb");
    ASSERT_TRUE(found);
    ASSERT_TRUE(iter->word() == "bbb");
    found = iter->incr();
    ASSERT_TRUE(iter->word() == "cccc");
  }
}

