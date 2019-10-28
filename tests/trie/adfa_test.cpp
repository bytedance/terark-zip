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

  TEST(ADFA_TEST, EMPTY_DFA_TEST) {
    // MainPatricia trie(sizeof(uint32_t), 1<<20, Patricia::SingleThreadShared);
    MainPatricia dfa;
    std::unique_ptr<ADFA_LexIterator> iterU(dfa.adfa_make_iter(initial_state));
    auto iter = iterU.get();
    assert(!iter->seek_begin());
    assert(!iter->seek_end());
    assert(!iter->seek_lower_bound("1"));
    assert(!iter->seek_lower_bound("2"));
    assert(!iter->seek_lower_bound("9"));
    assert(!iter->seek_lower_bound("\xFF"));
  }

  TEST(ADFA_TEST, ITERATOR_TEST) {

  }
}

