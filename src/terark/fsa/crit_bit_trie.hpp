#include <queue>
#include <terark/histogram.hpp>
#include <terark/rank_select.hpp>
#include <terark/util/crc.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/sorted_uint_vec.hpp>

namespace terark {

class CritBitTriePackedBuilder;
class CritBitTriePacked;
class CritBitTrieBuilder;
class CritBitTrie;
struct IndexCBTPrefixHeader {
  uint64_t magic : 8;
  uint64_t header_size : 24;
  uint64_t header_crc16 : 16;
  uint64_t version : 4;
  uint64_t reserve_bits : 11;
  uint64_t extra_header : 1;
};

// prefix builder class
class CritBitTriePackedBuilder {
 public:
  CritBitTriePackedBuilder() = default;
  CritBitTriePackedBuilder(size_t numKeys, size_t entryPerTrie,
                           size_t sumKeyLen, bool isReverse,
                           uint8_t hash_bit_num);
  CritBitTriePackedBuilder(const CritBitTriePackedBuilder&);
  CritBitTriePackedBuilder& operator=(const CritBitTriePackedBuilder&);
  ~CritBitTriePackedBuilder();
  void insert(fstring key, size_t pos);
  void encode();
  size_t num_words() const { return num_words_; }
  size_t trie_nums() const { return trie_nums_; }
  size_t total_key_size() const { return total_key_size_; }
  CritBitTriePacked* newcbt();
  void get_bounds(bool reverse, fstrvec*);
  CritBitTrieBuilder& operator[](int i) {
    assert(i >= 0);
    assert(uint64_t(i) < trie_nums_);
    return builder_list_[i];
  }
  uint64_t entry_per_trie_;
  uint64_t trie_nums_;
  uint64_t num_words_;
  uint64_t total_key_size_;
  uint64_t max_layer_;
  uint8_t hash_bit_num_;
  std::vector<CritBitTrieBuilder> builder_list_;
  SortedUintVec header_vec;
  bool is_reverse_;
};

class CritBitTrieBuilder {
 public:
  CritBitTrieBuilder(bool is_reverse, uint8_t hash_bit_num)
      : is_reverse_(is_reverse),
        hash_bit_num_(hash_bit_num),
        hash_mask_(hash_bit_num ? (uint64_t(-1) >> (64 - hash_bit_num)) : 0) {}
  ~CritBitTrieBuilder();
  void insert(fstring key);
  void encode();
  void compress_diff_bit_array();
  uint64_t trie_size() const { return encoded_trie_.mem_size(); }
  uint64_t layer() const { return layer_; };

  static bool test_key(fstring key, uint64_t bit);
  static uint64_t comp_key(fstring key, fstring key2);

  rank_select_il encoded_trie_;
  valvec<uint64_t> diff_bit_array_;
  struct Node {
    uint32_t child[2];
    uint64_t diff_bit;
  };
  Node* root() { return &node_storage_[0]; }
  Node* at(size_t pos) { return &node_storage_[pos]; }
  bool is_reverse_;
  valvec<Node> node_storage_;
  uint64_t layer_;
  uint32_t root_pos_ = uint32_t(-1);
  uint64_t base_bit_num_;
  uint64_t extra_bit_num_;
  uint8_t hash_bit_num_;
  uint64_t hash_mask_;
  UintVecMin0 base_;
  rank_select_il bitmap_;
  UintVecMin0 extra_;
  UintVecMin0 hash_vec_;
  valvec<byte> prev_key_;
  valvec<byte> smallest_key_;
  valvec<byte> largest_key_;
  valvec<uint64_t> diff_bit_delta_;
};

// prefix class
class CritBitTriePacked {
 public:
  CritBitTriePacked() = default;

  uint64_t entry_per_trie_;
  uint64_t trie_nums_;
  uint64_t num_words_;
  uint64_t max_layer_;
  uint8_t hash_bit_num_;

  SortedUintVec header_vec;
  std::vector<CritBitTrie> trie_list_;

  void save(std::function<void(const void*, size_t)> append) const;
  void load(fstring mem);
  void clear();
  void risk_release();

  size_t base_rank_id(int trie_index) const {
    return trie_index * entry_per_trie_;
  }
  size_t num_words() const { return num_words_; }
  size_t trie_nums() const { return trie_nums_; }
  CritBitTrie& operator[](int i) {
    assert(i >= 0);
    assert(uint64_t(i) < trie_nums_);
    return trie_list_[i];
  }
  const CritBitTrie& operator[](int i) const {
    assert(i >= 0);
    assert(uint64_t(i) < trie_nums_);
    return trie_list_[i];
  }
  uint64_t max_layer() const { return max_layer_; }
  uint8_t hash_bit_num() const { return hash_bit_num_; }

  uint64_t get_largest_id(size_t trie_index) const;
  uint64_t get_smallest_id(size_t trie_index) const;
};

class CritBitTrie {
 public:
  struct PathElement {
    uint64_t is_right : 1;
    uint64_t id : 63;
  };
  using Path = valvec<PathElement>;

  CritBitTrie() = default;
  static bool test_key(fstring key, uint64_t bit);
  size_t index(fstring key, Path* vec) const;
  size_t lower_bound(fstring key, fstring best_match_key, const Path& path,
                     int c) const;
  uint64_t make_diff_bit(uint64_t rank, uint64_t diff_base) const;
  bool hash_match(fstring key, uint64_t id, uint8_t hash_bit_num) const;

  void risk_release_ownership();
  void calculat_layer_pos();

  uint64_t base_bit_num_;
  uint64_t extra_bit_num_;
  uint64_t layer_;

  rank_select_il encoded_trie_;
  UintVecMin0 base_;
  rank_select_il bitmap_;
  UintVecMin0 extra_;
  UintVecMin0 hash_vec_;

  valvec<uint64_t> layer_id_;
  uint64_t* layer_rank_;
};
}  // namespace terark