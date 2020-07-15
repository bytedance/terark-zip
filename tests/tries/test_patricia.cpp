//
// Created by leipeng on 2020/7/15.
//
#include <terark/fsa/cspptrie.hpp>
#include <set>

using namespace terark;

int main() {
  std::unique_ptr<Patricia> trie(
      Patricia::create(sizeof(void*), 4<<20, Patricia::MultiWriteMultiRead));
  std::set<std::string> stdset;
  bool ret_ok = false;
  const char* val_obj = NULL;
  const char* val_got = NULL;
  auto val_of = [&](const char* x) { val_obj = x; return &val_obj; };
  auto rtok = trie->tls_reader_token();
  auto wtok = trie->tls_writer_token_nn();
  auto iter = trie->new_iter();

  auto check_all = [&]() {
    if (iter->seek_begin()) {
      do {
        auto ki = aligned_load<const char*>(iter->value());
        TERARK_VERIFY(iter->word() == ki);
        TERARK_VERIFY(stdset.find(ki) != stdset.end());
      } while (iter->incr());
    }
    for (const auto& key : stdset) {
        rtok->acquire(trie.get());
        TERARK_VERIFY(rtok->lookup(key));
        TERARK_VERIFY(key == aligned_load<const char*>(rtok->value()));
        TERARK_VERIFY(iter->seek_lower_bound(key));
        TERARK_VERIFY(key == aligned_load<const char*>(iter->value()));
        rtok->release();
    }
    iter->idle();
  };
  wtok->acquire(trie.get());

#define DO_INSERT(key) do { \
    stdset.insert(key); \
    ret_ok = trie->insert(key, val_of(key), wtok); \
    val_got = aligned_load<char*>(wtok->value()); \
    assert(val_got == val_obj); \
    trie->sync_stat(); \
    assert(stdset.size() == trie->num_words()); \
    check_all(); \
  } while (0)

  DO_INSERT("aaaabbbbcccc"); assert(ret_ok);
  assert(trie->trie_stat().n_add_state_move == 1);
  DO_INSERT("aaaabbbb"); assert(ret_ok); // split
  assert(trie->trie_stat().n_split == 1);

  DO_INSERT("aaaabbbb"); assert(!ret_ok); // dup, fail

  DO_INSERT("aaaac"); assert(ret_ok);
  assert(trie->trie_stat().n_fork == 1);

  DO_INSERT("aaaad"); assert(ret_ok);
  assert(trie->trie_stat().n_add_state_move == 2);

  DO_INSERT("aaaade"); assert(ret_ok);
  assert(trie->trie_stat().n_add_state_move == 3);

  DO_INSERT("aaaa"); assert(ret_ok);
  assert(trie->trie_stat().n_mark_final == 1);

  DO_INSERT(""); assert(ret_ok);
  assert(trie->trie_stat().n_mark_final == 2);

  DO_INSERT("aaaabb"); assert(ret_ok);
  assert(trie->trie_stat().n_split == 2);

  DO_INSERT("aaaab"); assert(ret_ok);
  assert(trie->trie_stat().n_split == 3);

  DO_INSERT("aaaabbbbccc"); assert(ret_ok);
  assert(trie->trie_stat().n_split == 4);

  DO_INSERT("bb"); assert(ret_ok);
  assert(trie->trie_stat().n_add_state_move == 4);
  size_t i = 0;
  for (char ch : fstring("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-*/")) {
    char buf[16];
    snprintf(buf, sizeof(buf), "bb%c", ch);
    const char* dup = strdup(buf); // intentional leak memory
    DO_INSERT(dup); assert(ret_ok);
    TERARK_VERIFY_F(trie->trie_stat().n_add_state_move == 5 + i, "%zd %zd",
                    trie->trie_stat().n_add_state_move ,  5 + i);
    i++;
  }

  wtok->release();
  iter->dispose();

  return 0;
}
