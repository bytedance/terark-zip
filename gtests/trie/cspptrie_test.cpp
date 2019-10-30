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

  void load_data(hash_strmap<>& strVec) {
    // TODO, read from different files
    strVec.insert_i("a");
    strVec.insert_i("b");
    strVec.insert_i("ccc");
  }

  TEST(CSPPTRIE_TEST, BUILD_TRIE) {
		// inserter
		auto insert = [](MainPatricia& trie, const hash_strmap<>& strVec) {
			MainPatricia::WriterToken token(&trie);
			for (size_t i = 0, n = strVec.end_i(); i < n; i++) {
					fstring key = strVec.key(i);
					token.update_lazy();
					trie.insert(key, NULL, &token);
			}
		};

    MainPatricia patricia;
    hash_strmap<> strVec;
    load_data(strVec);
    size_t lineno = strVec.size();

    printf("done, lines=%zd...\n", lineno);
    if (strVec.size() > 0) {
        insert(patricia, strVec);
        minimize_and_path_zip(patricia);
        printf("strVec.sort_slow()...\n");
        strVec.sort_slow();
        printf("strVec.key(0).size() = %zd\n", strVec.key(0).size());
    }
    // 
  }
}

