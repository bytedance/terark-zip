#pragma once
#include <gtest/gtest.h>
#include <getopt.h>
#include <random>

#include "terark/util/function.hpp"
#include "terark/util/linebuf.hpp"
#include "terark/util/profiling.hpp"
#include "terark/hash_strmap.hpp"
#include "terark/fsa/cspptrie.inl"
#include "terark/fsa/nest_trie_dawg.hpp"


namespace terark {

  template<typename DFA, typename Inserter>
  void buil_dfa(Inserter inserter) {
    DFA dfa;
  }

  TEST(ADFA_TEST, EMPTY_DFA_TEST_1) {
    // MainPatricia trie(sizeof(uint32_t), 1<<20, Patricia::SingleThreadShared);
    MainPatricia dfa;
    std::unique_ptr<ADFA_LexIterator> iterU(dfa.adfa_make_iter(initial_state));
    auto iter = iterU.get();
    ASSERT_TRUE(!iter->seek_begin());
    ASSERT_TRUE(!iter->seek_end());
    ASSERT_TRUE(!iter->seek_lower_bound("1"));
    ASSERT_TRUE(!iter->seek_lower_bound("2"));
    ASSERT_TRUE(!iter->seek_lower_bound("9"));
    ASSERT_TRUE(!iter->seek_lower_bound("\xFF"));
  }

  TEST(ADFA_TEST, ITERATOR_TEST) {

  }
}

