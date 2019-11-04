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

  TEST(CSPPTRIE_TEST, BUILD_SIMPLE_TRIE) {
		// inserter
		auto insert = [](MainPatricia& trie, const hash_strmap<>& strVec) {
			MainPatricia::WriterToken token(&trie);
			for (size_t i = 0, n = strVec.end_i(); i < n; i++) {
					fstring key = strVec.key(i);
					token.update_lazy();
          uint32_t v = 123;
					trie.insert(key, &v, &token);
			}
		};

    // MainPatricia(offset_size, max_value_capacity, access_mode)
    MainPatricia patricia(sizeof(uint32_t), 10*1024*1024, Patricia::SingleThreadShared);
    hash_strmap<> strVec;
    load_data(strVec);
    size_t lineno = strVec.size();

    printf("done, lines=%zd...\n", lineno);
    if (strVec.size() > 0) {
        insert(patricia, strVec);
        // TODO
        // minimize_and_path_zip(patricia);
        printf("strVec.sort_slow()...\n");
        strVec.sort_slow();
        printf("strVec.key(0).size() = %zd\n", strVec.key(0).size());
    }

    // reader
    std::unique_ptr<ADFA_LexIterator> iterADFA(patricia.adfa_make_iter(initial_state));
    auto iter = iterADFA.get();
    bool has_data = iter->seek_begin();

    // read all data check existence
    ASSERT_TRUE(has_data);
    for(size_t i = 0; i < lineno; ++i) {
      auto restored_word = iter->word();
      auto origin_word = strVec.key(i);
      ASSERT_TRUE(restored_word == origin_word);
      printf("origin_word = %s\n", origin_word.data());
      if( i < lineno - 1) {
        ASSERT_TRUE(iter->incr());
      }
    }

    // check point search
    has_data = iter->seek_lower_bound("b");
    ASSERT_TRUE(has_data);
    ASSERT_TRUE(iter->word() == "b");
    printf("word_state = %d\n", iter->word_state());
    // printf("word_state->valptr = %d\n", iter->get_valptr(iter->word_state()));


    // check point search not exist
    has_data = iter->seek_lower_bound("bbb");
    ASSERT_TRUE(has_data); // find `ccc`
    ASSERT_TRUE(iter->word() != "bbb");

    // get value from MainPatricia without secondary pointer
    auto reader_token = patricia.acquire_tls_reader_token();
    auto found = patricia.lookup("bbb", reader_token);
    ASSERT_TRUE(!found);
    found = patricia.lookup("b", reader_token);
    uint32_t actual_val = *(uint32_t *) reader_token->value();
    printf("offset: %d\n", actual_val);
    ASSERT_TRUE(found);
  }


  /**
   * Two different ways to use patricia mem:
   *  1. construct value before token inserton
   *      easy to use & straightforward
   *
   *  2. construct value after token insertion
   *      little bit complex but allow us to make sure the token was successfully inserted before construct value object.
   */
  TEST(CSPPTRIE_TEST, SECONDARY_ACCESS_TRIE) {
    MainPatricia patricia(sizeof(uint32_t), 10*1024*1024, Patricia::SingleThreadShared);
    
    // allocate actual value space and store val_loc as trie's value
		auto insert = [&](fstring& key, fstring& value) {
			// can also create a new token like this:
      /*
      if(patricia.tls_writer_token() == nullptr) {
        patricia.tls_writer_token().reset(new Patricia::WriterToken());
      }
      auto token = static_cast<Patricia::WriterToken*>(patricia.tls_writer_token().get());
      ASSERT_TRUE(token != nullptr);
      */
      MainPatricia::WriterToken token(&patricia);

      // allocate a memory space for the varient length value and get a location idx for it from trie
      uint32_t value_size = value.size();
      uint32_t value_loc = patricia.mem_alloc(value_size);
      memcpy((char*)patricia.mem_get(value_loc), value.data(), value_size);

      printf("val_loc after alloc: %d, data size = %d\n", value_loc, value_size);
      // could direct insert into patrica, or use token to init value (in the second case, value_loc will be used as the result store)
      auto success = patricia.insert(key, &value_loc, &token);
      //ASSERT_TRUE(success);
    };

    fstring key = "key1";
    fstring value = "value12";
    insert(key, value);

    auto reader_token = patricia.acquire_tls_reader_token();

    // point search
    auto found = patricia.lookup("key1", reader_token);
    ASSERT_TRUE(found);
    uint32_t val_loc = *(uint32_t *) reader_token->value();
    ASSERT_TRUE(val_loc > 0);
    printf("val_loc = %d\n", val_loc);
    char data[8];
    memset(data, '\0', 8);
    memcpy(data, (char *) patricia.mem_get(val_loc), 7);
    printf("data = %s\n", data);

    // iterator, use cspptrie's, not adfa's
    // auto iter = reader_token->iter();
    MainPatricia::IterMem handle;
    handle.construct(&patricia);
    auto iter = handle.iter();
    ASSERT_TRUE(iter != nullptr);
    found = iter->seek_begin();
    while(found) {
      uint32_t loc = *(uint32_t*)iter->value();
      printf("iter loc found: %d\n", loc);
      found = iter->incr();
    }
  }

}
