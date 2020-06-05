#include <inttypes.h>

#include <random>
#include <string>
#include <terark/fsa/crit_bit_trie.hpp>
using namespace terark;

const size_t len = 1024;
// // rand_string =
std::string random_string(std::string::size_type length) {
  thread_local static std::mt19937 rg{std::random_device{}()};
  std::string s;
  s.resize(std::uniform_int_distribution<size_t>(0, length)(rg));
  std::generate(
      s.begin(), s.end(),
      std::bind(std::uniform_int_distribution<char>(-128, 127), std::ref(rg)));
  return s;
}

size_t Find(CritBitTriePacked* cbt_pack, fstring key, fstrvec& bounds,
            const std::vector<std::string>& rand_string) {
  size_t cbt_index =
      terark::lower_bound_0<const fstrvec&>(bounds, cbt_pack->trie_nums(), key);
  if (cbt_index == cbt_pack->trie_nums()) {
    return size_t(-1);
  }
  auto& cbt = (*cbt_pack)[cbt_index];
  size_t id_in_trie = cbt.index(key, nullptr);
  if (cbt_pack->hash_bit_num() > 0) {
    if (!cbt.hash_match(key, id_in_trie, cbt_pack->hash_bit_num())) {
      return size_t(-1);
    }
  }
  size_t suffix_id = id_in_trie + cbt_pack->base_rank_id(cbt_index);
  return rand_string[suffix_id] == key ? suffix_id : size_t(-1);
}

size_t DictRank(CritBitTriePacked* cbt_packed, fstring key, fstrvec& bounds,
                const std::vector<std::string>& rand_string) {
  size_t cbt_index = terark::lower_bound_0<const fstrvec&>(
      bounds, cbt_packed->trie_nums(), key);
  if (cbt_index == cbt_packed->trie_nums()) {
    return cbt_packed->num_words();
  }
  auto& cbt = (*cbt_packed)[cbt_index];
  valvec<CritBitTrie::PathElement> vec;
  vec.resize(100);
  size_t rank = cbt.index(key, &vec) + cbt_packed->base_rank_id(cbt_index);
  auto best_match_key = rand_string[rank];
  int c = fstring_func::compare3()(key, best_match_key);
  if (c != 0) {
    rank = cbt.lower_bound(key, best_match_key, vec, c) +
           cbt_packed->base_rank_id(cbt_index);
    assert(rank < cbt_packed->num_words());
  }
  return rank;
}

void test(std::vector<std::string>& rand_string,
          const std::vector<std::string>& test_string, size_t pack) {
  assert(std::is_sorted(rand_string.begin(), rand_string.end()));

  CritBitTriePackedBuilder packed_builder(rand_string.size(), pack, 0, false,
                                          0);
  for (size_t i = 0; i < rand_string.size(); ++i) {
    packed_builder.insert(rand_string[i], i / pack);
  }
  packed_builder.encode();
  fstrvec bounds;
  CritBitTriePacked* cbt_pack = packed_builder.newcbt();
  packed_builder.get_bounds(false, &bounds);
  // printf("succient :");
  // for (size_t i = 0; i < cbt.encoded_trie_.size(); ++i) {
  //   printf("%d ", cbt.encoded_trie_.is0(i) ? 0 : 1);
  // }
  // printf("\n");
  size_t id;

  auto verify = [&](const std::string& k) {
    do {
      id = Find(cbt_pack, k, bounds, rand_string);
      if (id == size_t(-1)) {
        if (std::binary_search(rand_string.begin(), rand_string.end(), k)) {
          break;
        }
      } else if (k != rand_string[id]) {
        break;
      }
      id = DictRank(cbt_pack, k, bounds, rand_string);
      if (rand_string.begin() + id !=
          std::lower_bound(rand_string.begin(), rand_string.end(), k)) {
        break;
      }
      return false;
    } while (false);
    printf("got error\n");
    return true;
  };
  for (auto s : rand_string) {
    while (verify(s))
      ;
    while (verify(s + ' '))
      ;
    if (!s.empty()) {
      s.back() = char(255);
      while (verify(s))
        ;
      while (verify(std::string(s.data(), s.size() - 1)))
        ;
    }
  }
  for (auto& s : test_string) {
    while (verify(s))
      ;
  }
}

void test_group(std::vector<std::string>& rand_string,
                const std::vector<std::string>& test_string) {
  std::sort(rand_string.begin(), rand_string.end());
  rand_string.resize(std::unique(rand_string.begin(), rand_string.end()) -
                     rand_string.begin());

  test(rand_string, test_string, 65536);
  test(rand_string, test_string, rand_string.size());
  if (rand_string.size() > 1) {
    test(rand_string, test_string, rand_string.size() / 2);
    test(rand_string, test_string, rand_string.size() - 1);
  }
  if (rand_string.size() > 2) {
    test(rand_string, test_string, rand_string.size() / 2 + 1);
    test(rand_string, test_string, rand_string.size() - 2);
  }
  if (rand_string.size() > 3) {
    test(rand_string, test_string, rand_string.size() / 2 - 1);
  }
}

int main(int argc, const char** argv) {
  std::vector<std::string> case1 = {"A",  "AA", "AB", "ABC", "ABCD",
                                    "AD", "B",  "BA", "BB",  "BBA"};
  std::vector<std::string> case2 = {
      "A", "AA", "AAA", "AAAQ", "AB", "ABB", "ABC", "ABCD", "AC",  "AZ",
      "B", "BA", "BB",  "BBA",  "CA", "CB",  "CQ",  "QA",   "QAA", "QAAAA"};

  test_group(case1, {"AD"});
  test_group(case2, {"QAAA", "QAAB", "QAAAAA"});

  size_t test_times = 100;
  size_t test_string_num = 1000;
  for (size_t j = 0; j < test_times; ++j) {
    std::vector<std::string> rand_string;
    std::vector<std::string> test_string;
    for (size_t i = 0; i < test_string_num; ++i) {
      rand_string.push_back(random_string(len));
      test_string.push_back(random_string(len));
    }
    test_group(rand_string, test_string);
    printf("OK\n");
  }
  return 0;
}