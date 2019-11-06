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

  TEST(NLT_TEST, SIMPLE_TEST) {
  }


  template<typename T>
  void load_keys(T& t) {
    t.push_back("aaa");
    t.push_back("bbb");
    t.push_back("cccc");
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

    // prepare input vector
    StrVecType records;
    load_keys(records);
    ASSERT_TRUE(records[0] == "aaa");
    ASSERT_TRUE(records[2] == "cccc");
  }

}

