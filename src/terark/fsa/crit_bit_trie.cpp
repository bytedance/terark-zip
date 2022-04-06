#include "crit_bit_trie.hpp"
#include "terark/stdtypes.hpp"

#include <inttypes.h>

#include <cstring>
namespace {
static constexpr uint32_t invalid_pos = uint32_t(-1);
}  // namespace

namespace terark {
class SortedUintVec;

CritBitTriePackedBuilder::CritBitTriePackedBuilder(size_t numKeys,
                                                   size_t entryPerTrie,
                                                   size_t sumKeyLen,
                                                   bool isReverse,
                                                   uint8_t hash_bit_num)
    : entry_per_trie_(entryPerTrie),
      trie_nums_((numKeys + entryPerTrie - 1) / entryPerTrie),
      num_words_(numKeys),
      total_key_size_(sumKeyLen),
      hash_bit_num_(hash_bit_num),
      is_reverse_(isReverse) {
    CritBitTrieBuilder init(isReverse, hash_bit_num);
    builder_list_.resize(trie_nums_, init);
}

void CritBitTriePackedBuilder::insert(fstring key, size_t pos) {
  builder_list_[pos].insert(key);
}

CritBitTriePacked* CritBitTriePackedBuilder::newcbt() {
  std::unique_ptr<SortedUintVec::Builder> builder(
      SortedUintVec::createBuilder(false, 64));
  std::unique_ptr<CritBitTriePacked> trie(new CritBitTriePacked);
  trie->trie_list_.resize(trie_nums_);
  trie->max_layer_ = 0;
  // 0   num_words
  builder->push_back(num_words_);
  trie->num_words_ = num_words_;
  // 1   trie_nums
  builder->push_back(trie_nums_);
  trie->trie_nums_ = trie_nums_;
  // 2   entry per trie
  builder->push_back(entry_per_trie_);
  trie->entry_per_trie_ = entry_per_trie_;
  // 3   hash_bit_num
  builder->push_back(hash_bit_num_);
  trie->hash_bit_num_ = hash_bit_num_;

  for (size_t i = 0; i < trie_nums_; ++i) {
    auto& b = builder_list_[i];
    b.largest_key_.swap(b.prev_key_);
    if (is_reverse_) {
      b.largest_key_.swap(b.smallest_key_);
    }
    // max_layer_ isn't saved
    trie->max_layer_ = std::max(trie->max_layer_, b.layer_);
    auto& cbt = (*trie)[i];
    // 4    base bit num
    cbt.base_bit_num_ = b.base_bit_num_;
    builder->push_back(cbt.base_bit_num_);
    // 5    extra bit num
    cbt.extra_bit_num_ = b.extra_bit_num_;
    builder->push_back(cbt.extra_bit_num_);
    // 6    layer
    cbt.layer_ = b.layer_;
    builder->push_back(cbt.layer_);
    // 7    encoded trie mem size
    cbt.encoded_trie_.swap(b.encoded_trie_);
    builder->push_back(cbt.encoded_trie_.mem_size());
    // 8    base size and mem_size
    cbt.base_.swap(b.base_);
    builder->push_back(cbt.base_.mem_size());
    // 9    bitmap mem size
    cbt.bitmap_.swap(b.bitmap_);
    builder->push_back(cbt.bitmap_.mem_size());
    // 10   mem_size (extra size = bitmap.max_rank1())
    b.extra_.shrink_to_fit();
    cbt.extra_.swap(b.extra_);
    builder->push_back(cbt.extra_.mem_size());
    // 11   hash vec mem_size (size = base.size())
    if (hash_bit_num_ != 0) {
      if (is_reverse_) {
        cbt.hash_vec_.resize_with_uintbits(b.hash_vec_.size(), hash_bit_num_);
        for (size_t i = 0, j = b.hash_vec_.size() - 1; i < b.hash_vec_.size();
             ++i, --j) {
          cbt.hash_vec_.set_wire(j, b.hash_vec_[i]);
        }
      } else {
        b.hash_vec_.shrink_to_fit();
        cbt.hash_vec_.swap(b.hash_vec_);
      }
    }
    builder->push_back(cbt.hash_vec_.mem_size());
    cbt.calculat_layer_pos();
  }

  builder->finish(&header_vec);
  trie->header_vec.swap(header_vec);
  return trie.release();
}

void CritBitTriePackedBuilder::get_bounds(bool reverse, fstrvec* bounds) {
  bounds->erase_all();
  for (size_t i = 0; i < trie_nums_; ++i) {
    auto& b = builder_list_[i];
    if (reverse) {
      bounds->push_back(b.smallest_key_);
    } else {
      bounds->push_back(b.largest_key_);
    }
  }
}

void CritBitTriePacked::clear() {
  header_vec.clear();
  trie_list_.clear();
}

void CritBitTriePacked::risk_release() {
  header_vec.risk_release_ownership();
  for (auto& trie : trie_list_) {
    trie.risk_release_ownership();
  }
  trie_list_.clear();
}

uint64_t CritBitTriePacked::get_largest_id(size_t trie_index) const {
  uint64_t rank = entry_per_trie_ * (trie_index + 1) - 1;
  return std::min(rank, num_words_ - 1);
}

CritBitTriePackedBuilder::~CritBitTriePackedBuilder() {}

void CritBitTriePackedBuilder::encode() {
  max_layer_ = 0;
  for (auto& builder : builder_list_) {
    builder.node_storage_.pop_back();
    builder.encode();
    builder.compress_diff_bit_array();
    max_layer_ = std::max(max_layer_, builder.layer_);
  }
}

void CritBitTriePacked::save(
    std::function<void(const void*, size_t)> append) const {
  IndexCBTPrefixHeader header;
  memset(&header, 0, sizeof header);
  header.header_size = header_vec.mem_size();
  header.magic = 0;
  header.version = 0;
  header.header_crc16 =
      Crc16c_update(0, header_vec.data(), header_vec.mem_size());
  header.extra_header = 0;
  header.reserve_bits = 0;
  append(&header, sizeof header);
  append(header_vec.data(), header_vec.mem_size());
  for (auto& trie : trie_list_) {
    append(trie.encoded_trie_.data(), trie.encoded_trie_.mem_size());
    append(trie.base_.data(), trie.base_.mem_size());
    append(trie.bitmap_.data(), trie.bitmap_.mem_size());
    append(trie.extra_.data(), trie.extra_.mem_size());
    append(trie.hash_vec_.data(), trie.hash_vec_.mem_size());
  }
}
/**
 *   0   num_words
 *   1   trie_nums
 *   2   entry per trie
 *   3   hash bit number
 *   4 ~ 11 base_bit_num extra_bit_num layer succient_mem_size base_mem_size
 *          bitmap_mem_size extra_mem_size hash_mem_size
 */
bool CritBitTriePacked::load(fstring mem) {
  const int mem_index_begin = 4;
  const int num_each_group = 8;
  const IndexCBTPrefixHeader* header =
      reinterpret_cast<const IndexCBTPrefixHeader*>(mem.data());

  byte_t *header_data = (byte_t *) mem.data() + sizeof(IndexCBTPrefixHeader);
  if (mem.size() < sizeof(IndexCBTPrefixHeader) ||
      mem.size() < header->header_size + sizeof(IndexCBTPrefixHeader) ||
      header->header_crc16 != Crc16c_update(0, header_data, header->header_size)) {
      return false;
  }
  header_vec.risk_set_data(header_data, header->header_size);
  num_words_ = header_vec.get(0);
  trie_nums_ = header_vec.get(1);
  entry_per_trie_ = header_vec.get(2);
  hash_bit_num_ = header_vec.get(3);
  max_layer_ = 0;
  // init CritBitTriePacked
  trie_list_.resize(trie_nums_);
  byte_t* data_ptr =
      (byte_t*)mem.data() + sizeof(*header) + header_vec.mem_size();
  for (size_t i = 0; i < trie_nums_; ++i) {
    size_t begin = mem_index_begin + i * num_each_group;
    uint64_t base_size =
        i + 1 < trie_nums_ ? entry_per_trie_ : num_words_ % entry_per_trie_;
    auto& t = trie_list_[i];

    t.base_bit_num_ = header_vec.get(begin++);
    t.extra_bit_num_ = header_vec.get(begin++);
    t.layer_ = header_vec.get(begin++);
    max_layer_ = std::max(max_layer_, t.layer_);

    size_t encoded_trie_mem_size = header_vec.get(begin++);
    size_t base_mem_size = header_vec.get(begin++);
    size_t bitmap_mem_size = header_vec.get(begin++);
    size_t extra_mem_size = header_vec.get(begin++);
    size_t hash_vec_mem_size = header_vec.get(begin++);
    assert((hash_bit_num_ != 0 && hash_vec_mem_size != 0) ||
           (hash_bit_num_ == 0 && hash_vec_mem_size == 0));
    t.encoded_trie_.risk_mmap_from(data_ptr, encoded_trie_mem_size);
    data_ptr += encoded_trie_mem_size;

    t.base_.risk_set_data(data_ptr, base_size - 1, t.base_bit_num_);
    data_ptr += base_mem_size;

    t.bitmap_.risk_mmap_from(data_ptr, bitmap_mem_size);
    data_ptr += bitmap_mem_size;

    t.extra_.risk_set_data(data_ptr, t.bitmap_.max_rank1(), t.extra_bit_num_);
    data_ptr += extra_mem_size;

    if (hash_bit_num_ != 0) {
      t.hash_vec_.risk_set_data(data_ptr, base_size, hash_bit_num_);
      data_ptr += hash_vec_mem_size;
    }
    t.calculat_layer_pos();
  }
  return true;
}

CritBitTrieBuilder::~CritBitTrieBuilder() {}

void CritBitTrieBuilder::compress_diff_bit_array() {
  Uint64Histogram hist_delta;
  // 1st isn't used
  if (!diff_bit_array_.empty()) {
    // fill the diff_bit_delta_ and hist_delta
    diff_bit_delta_.push_back(diff_bit_array_[0]);
    hist_delta[diff_bit_array_[0]]++;
  }
  for (size_t i = 0; i < diff_bit_array_.size(); ++i) {
    uint64_t parent_rank = i;
    uint64_t child_pos = parent_rank * 2;
    if (encoded_trie_.is1(child_pos)) {
      uint64_t child_rank = encoded_trie_.rank1(child_pos + 1);
      assert(diff_bit_array_[child_rank] >= diff_bit_array_[parent_rank]);
      uint64_t delta =
          diff_bit_array_[child_rank] - diff_bit_array_[parent_rank];
      diff_bit_delta_.push_back(delta);
      hist_delta[delta]++;
    }
    child_pos = parent_rank * 2 + 1;
    if (encoded_trie_.is1(child_pos)) {
      uint64_t child_rank = encoded_trie_.rank1(child_pos + 1);
      assert(diff_bit_array_[child_rank] >= diff_bit_array_[parent_rank]);
      uint64_t delta =
          diff_bit_array_[child_rank] - diff_bit_array_[parent_rank];
      diff_bit_delta_.push_back(delta);
      hist_delta[delta]++;
    }
  }
  hist_delta.finish();
  uint64_t max_delta = hist_delta.m_max_key_len;
  uint64_t total_storage_size = uint64_t(-1);
  uint64_t max_delta_bit_num = UintVecMin0::compute_uintbits(max_delta);
  base_bit_num_ = 0;
  extra_bit_num_ = 0;
  // make out the base_bit_num
  for (size_t i = 1; i <= max_delta_bit_num; ++i) {
    // i is the bit num of base_
    uint64_t max_base = uint64_t(-1) >> (64 - i);
    uint64_t below_num = 0;
    auto below_sum = [&below_num, &max_base](uint64_t key, size_t num) {
      if (key < max_base) below_num += num;
    };
    hist_delta.for_each(below_sum);
    uint64_t upper_num = hist_delta.m_cnt_sum - below_num;
    uint64_t tmp =
        hist_delta.m_cnt_sum * i + upper_num * (max_delta_bit_num - i);
    if (tmp < total_storage_size) {
      total_storage_size = tmp;
      base_bit_num_ = i;
    }
  }
  extra_bit_num_ = max_delta_bit_num - base_bit_num_;
  uint64_t base_max = uint64_t(-1) >> (64 - base_bit_num_);
  base_.resize_with_uintbits(diff_bit_delta_.size(), base_bit_num_);
  extra_.resize_with_uintbits(0, extra_bit_num_);
  bitmap_.resize(diff_bit_delta_.size());
  for (size_t i = 0; i < diff_bit_delta_.size(); ++i) {
    uint64_t delta = diff_bit_delta_[i];
    if (delta <= base_max) {
      base_.set_wire(i, delta);
      bitmap_.set0(i);
    } else {
      base_.set_wire(i, delta & base_max);
      bitmap_.set1(i);
      extra_.push_back(delta >> base_bit_num_);
    }
  }
  bitmap_.build_cache(false, false);
}

uint64_t CritBitTrieBuilder::comp_key(fstring key, fstring key2) {
  size_t min_len = std::min(key.size(), key2.size());
  size_t diff_byte;
  for (diff_byte = 0; diff_byte < min_len; ++diff_byte) {
    if (key[diff_byte] != key2[diff_byte]) {
      break;
    }
  }
  if (diff_byte == min_len) {
    // key is sub_str of prev_key(vice versa)
    // byte * 9 + 0
    return diff_byte * 9;
  } else {
    assert(diff_byte < min_len);
    // byte * 9 + diff_bit_in_byte
    uint32_t b = uint32_t(uint8_t(key[diff_byte] ^ key2[diff_byte])) << 23;
    assert(b > 0);
    return diff_byte * 9 + fast_clz32(b);
  }
}

void CritBitTrieBuilder::encode() {
  /**
   * each node has 2 bit
   * add 1 bit to support encoded_trie_.rank0(encoded_trie_.size())
   */
  encoded_trie_.resize(node_storage_.size() * 2 + 1);
  std::queue<uint32_t> pos_queue;
  if (!node_storage_.empty()) {
    pos_queue.push(root_pos_);
  }
  uint32_t bit_pos = 0;
  layer_ = 0;
  while (!pos_queue.empty()) {
    uint32_t queue_size = pos_queue.size();
    for (uint32_t i = 0; i < queue_size; ++i) {
      uint32_t front = pos_queue.front();
      diff_bit_array_.push_back(node_storage_[front].diff_bit);
      pos_queue.pop();
      for (auto& child_pos : node_storage_[front].child) {
        if (child_pos != invalid_pos) {
          encoded_trie_.set(bit_pos, 1);
          pos_queue.push(child_pos);
        } else {
          encoded_trie_.set(bit_pos, 0);
        }
        ++bit_pos;
      }
    }
    ++layer_;
  }
  encoded_trie_.build_cache(false, false);
  // release memory
  valvec<CritBitTrieBuilder::Node>().swap(node_storage_);
}

void CritBitTrieBuilder::insert(fstring key) {
  if (hash_bit_num_ != 0) {
    hash_vec_.push_back(std::hash<fstring>()(key) & hash_mask_);
  }
  if (node_storage_.empty()) {
    prev_key_.assign(key.data(), key.size());
    smallest_key_.assign(key.data(), key.size());
    node_storage_.emplace_back();
    return;
  }
  assert(prev_key_ != key);
  uint32_t new_node_pos = node_storage_.size() - 1;
  Node* new_node = &node_storage_.back();

  new_node->diff_bit = comp_key(key, prev_key_);
  new_node->child[0] = new_node->child[1] = invalid_pos;

  if (root_pos_ == invalid_pos) {
    root_pos_ = new_node_pos;
    prev_key_.assign(key.data(), key.size());
    node_storage_.emplace_back();
    return;
  }
  /**
   * 1-2 A = 10      A        B        B        B        B
   * 2-3 B = 4      1 2  ->  / 3  ->  / \  ->  / \  ->  / \
   * 3-4 C = 15             A        A   C    A   D    A   D
   * 4-5 D = 8             1 2      1 2 3 4  1 2 / 5  1 2 / \
   * 5-6 E = 12                                 C        C   E
   *                                          3 4       3 4  5 6
   *
   * 1-2 A = 10      A        B        C        C        C
   * 2-3 B = 7      1 2  ->  / 3  ->  / 4  ->  / \  ->  / \
   * 3-4 C = 3              A        B        B   D    B   E
   * 4-5 D = 12            1 2      / 3      / 3 4 5  / 3 / 6
   * 5-6 E = 8                     A        A        A   D
   *                              1 2      1 2      1 2 4 5
   */
  uint32_t parent = invalid_pos;
  uint32_t child = root_pos_;
  bool is_leaf;
  while (true) {
    Node* node = at(child);
    if (new_node->diff_bit < node->diff_bit) {
      is_leaf = false;
      break;
    }
    uint32_t next_child = node->child[!is_reverse_];
    if (next_child == invalid_pos) {
      is_leaf = true;
      break;
    }
    parent = child;
    child = next_child;
  }
  if (is_leaf) {
    at(child)->child[!is_reverse_] = new_node_pos;
  } else {
    if (parent == invalid_pos) {
      root_pos_ = new_node_pos;
    } else {
      at(parent)->child[!is_reverse_] = new_node_pos;
    }
    new_node->child[is_reverse_] = child;
  }
  // preserve the last key
  prev_key_.assign(key.data(), key.size());
  node_storage_.emplace_back();
}

void CritBitTrie::risk_release_ownership() {
  encoded_trie_.risk_release_ownership();
  base_.risk_release_ownership();
  bitmap_.risk_release_ownership();
  extra_.risk_release_ownership();
  hash_vec_.risk_release_ownership();
}

/**
 *          ?
 *          |
 *         -1
 * bit  0
 *
 *          ?
 *          |
 *          5
 *        /  \
 *      -1   -1
 * bit  1 0 0
 *
 *
 *              ?
 *              |
 *              3
 *           /     \
 *         4        6
 *       /   \    / \
 *      5    -1  10  -1
 *     / \      /  \
 *   -1  -1   -1   -1
 * pos      0 1 2 3 4 5 6 7 8 9
 * diff       3   4   6   5   10
 * bits     1 1 1 1 0 1 0 0 0 0 0
 * rank1    0 1 2 3 4 4 5 5 5 5 5 5
 * rank0    0 0 0 0 0 1 1 2 3 4 5 6
 */

void CritBitTrie::calculat_layer_pos() {
  // the last is leaf layer and layer + 1
  layer_id_.reserve((layer_ + 1) * 2);
  layer_id_.risk_set_size(layer_ + 1);
  layer_rank_ = layer_id_.data() + layer_id_.size();

  layer_id_[0] = 0;
  layer_rank_[0] = 0;
  if (encoded_trie_.size() == 0) {
    return;
  }
  uint64_t id = 0;
  uint64_t rank = 0;
  for (size_t layer = 1; layer < layer_id_.size(); ++layer) {
    uint64_t pos = (id + 1) * 2;
    rank = encoded_trie_.rank0(pos);
    id = pos - rank;
    layer_id_[layer] = id;
    layer_rank_[layer] = rank;
  }
}

bool CritBitTrie::test_key(fstring key, uint64_t bit_pos) {
  auto div_result = std::div(bit_pos, 9);
  if (size_t(div_result.quot) >= key.size()) {
    return false;
  }
  if (div_result.rem == 0) {
    return true;
  }
  return (uint32_t(key[div_result.quot]) >> (8 - div_result.rem)) & 1;
}

uint64_t CritBitTrie::make_diff_bit(uint64_t rank, uint64_t diff_base) const {
  uint64_t base_val = base_.get(rank) + diff_base;
  if (bitmap_.is0(rank)) {
    return base_val;
  } else {
    size_t idx = bitmap_.rank1(rank);
    return (extra_.get(idx) << base_bit_num_) + base_val;
  }
}

bool CritBitTrie::hash_match(fstring key, uint64_t id,
                             uint8_t hash_bit_num) const {
  assert(hash_bit_num <= 64);
  uint64_t hash_mask = uint64_t(-1) >> (64 - hash_bit_num);
  return (std::hash<fstring>()(key) & hash_mask) == hash_vec_.get(id);
}
/**
 *               3
 *            /      \
 *          4           6
 *       /     \       / \
 *      5      -1     10  -1
 *     / \           /  \
 *   -1  -1         -1  -1
 *
 * pos      0 1 2 3 4 5 6 7 8 9
 * bits     1 1 1 0 1 0 0 0 0 0
 * rank1
 * rank0
 *
 *               3
 *            /      \
 *          4           6
 *       /     \       / \
 *      5       7     10  -1
 *     / \     / \   /  \
 *   -1  -1  -1  -1 -1  -1
 *
 * pos      0 1 2 3 4 5 6 7 8 9
 * bits     1 1 1 1 1 0 0 0 0 0 0 0
 * rank1
 * rank0
 *
 *               3
 *            /      \
 *          4           6
 *       /     \       / \
 *     -1       7     10  -1
 *             / \   /  \
 *           -1  -1 -1  -1
 *
 * pos      0 1 2 3 4 5 6 7 8 9
 * bits     1 1 0 1 1 0 0 0 0 0
 * rank1
 * rank0
 *      id  pos rank
 *  1   1   0   0
 *  2   2   2   0
 *      break
 *  3   4   5   0
 *
 *
 *      -1
 * bit
 *
 *
 *          5
 *        /  \
 *      -1   -1
 * bit  0 0
 *
 */
size_t CritBitTrie::index(fstring key, Path* vec) const {
  if (vec != nullptr) {
    vec->risk_set_size(0);
  }
  if (base_.size() == 0) {
    return 0;
  }
  uint64_t pos;
  uint64_t id = 0;
  uint64_t rank = 0;
  uint64_t diff_base = 0;
  size_t layer = 0;
  while (true) {
#if 0
    uint64_t diff_bit = make_diff_bit(id, diff_base);
#else
    // manual inline make_diff_bit
    uint64_t diff_bit = base_.get(id) + diff_base;
    if (bitmap_.is1(id)) {
      diff_bit += extra_.get(bitmap_.rank1(id)) << base_bit_num_;
    }
#endif
    diff_base = diff_bit;
    size_t is_right = test_key(key, diff_bit);
    if (vec != nullptr) {
      assert(vec->size() < vec->capacity());
      PathElement e;
      e.is_right = is_right;
      e.id = id;
      vec->unchecked_push_back(e);
    }
    pos = id * 2 + is_right;
    // id = encoded_trie_.rank1(pos + 1);
    // for the performance, we will fix the id later
    id = encoded_trie_.rank1(pos);
    // rank += encoded_trie_.rank0(pos) - layer_rank_[layer];
    // ∵ encoded_trie_.rank0(pos) + encoded_trie_.rank1(pos) == pos
    // ∴   encoded_trie_.rank0(pos) - layer_rank_[layer];
    //   = (pos - encoded_trie_.rank1(pos)) - layer_rank_[layer];
    //   = (pos - id) - layer_rank_[layer];
    rank += (pos - id) - layer_rank_[layer];
    if (encoded_trie_.is1(pos)) {
      ++layer;
      // ∵ encoded_trie_.is1(pos)
      // ∴ encoded_trie_.rank1(pos) == encoded_trie_.rank1(pos + 1) - 1
      // we need
      //   id = encoded_trie_.rank1(pos + 1)
      // so, we need incr the id by 1 here ...
      ++id;
    } else {
      // ∵ encoded_trie_.is0(pos)
      // ∴ encoded_trie_.rank1(pos) == encoded_trie_.rank1(pos + 1)
      // we need
      //   id = encoded_trie_.rank1(pos + 1)
      // so, id is all right
      break;
    }
  }
  while (id != layer_id_[layer++]) {
    pos = (id + 1) * 2;
    id = encoded_trie_.rank1(pos);
    rank += (pos - id) - layer_rank_[layer];
  }
  return rank;
}
/**
 *              7
 *           /      \
 *         9          -1
 *       /    \
 *     -1      16
 *           /    \
 *         18      18
 *       /   \    /   \
 *     -1    -1  -1    26
 *                   /   \
 *                 -1     27
 *                       /   \
 *                      -1    -1
 *
 *
 *
 *              7
 *           /      \
 *         9          9
 *       /    \      /   \
 *     -1      15   -1    16
 *           /    \      /   \
 *         16     -1   -1    18
 *       /    \             /  \
 *      -1     18          -1   -1
 *           /   \
 *         -1    27
 *              /  \
 *             -1   -1
 *
 *    diff bit array : 7 9 9 15 16 16 18 18 27
 *    diff bit delta : 7 2 2 6 7 1 2 2 9
 *    layer id : 0 2 4 6 7 8 8
 *    layer rank : 0 0 2 4 7 8 10
 *    succient :1 1 0 1 0 1 1 0 0 1 0 1 0 0 0 1 0 0
 *
 *
 *              7
 *           /      \
 *         9          9
 *       /    \      /   \
 *     -1      15   -1    16
 *           /    \      /   \
 *         16     -1   -1    18
 *       /    \             /  \
 *      -1     18          -1   -1
 *           /   \
 *          27   -1
 *         /  \
 *        -1   -1
 *
 *     pos    0   2   4   6   8   10  12  14  16  18
 *     bits   1 1 0 1 0 1 1 0 0 1 0 1 0 0 1 0 0 0
 *     rank0  0 0 0 1 1 2 2 2 3 4 4 5 5 6 7 7 8 9 10
 *     rank1  0 1 2 2 3 3 4 5 5 5 6 6 7 7 7 8 8 8 8
 *     layer  |   |       |       |       |   |   |
 *     l_id   0   2       4       6       7   8   8
 *     l_rank 0   0       2       4       7   8   10
 *
 *
 *     step     id  pos rank
 *          1   1   0   0
 *          2   2   2   0
 *          3   3   5
 *          4   5   6
 *          5   7   11
 *          6   8   14
 *     break
 *          1       18
 *
 *
 */

/**
 *              7
 *           /      \
 *         9          -1
 *       /    \
 *     -1      15
 *           /    \
 *        16      -1
 *      /    \
 *    18      17
 *   /  \    /  \
 *  -1  -1  18   -1
 *         /  \
 *        -1   26
 *           /   \
 *          -1    27
 *               /  \
 *             -1   -1
 *
 */

size_t CritBitTrie::lower_bound(fstring key, fstring best_match_key,
                                const Path& path, int c) const {
  size_t common_bits = CritBitTrieBuilder::comp_key(key, best_match_key);
  uint64_t rank_inc = c > 0;
  if (path.empty()) {
    return rank_inc;
  }
  uint64_t id = (assert(path[0].id == 0), 0);
  uint64_t rank = 0;
  uint64_t diff_base = 0;
  size_t layer = 0;
  while (true) {
#if 0
    uint64_t diff_bit = make_diff_bit(id, diff_base);
#else
    // manual inline make_diff_bit
    uint64_t diff_bit = base_.get(id) + diff_base;
    if (bitmap_.is1(id)) {
      diff_bit += extra_.get(bitmap_.rank1(id)) << base_bit_num_;
    }
#endif
    diff_base = diff_bit;
    if (diff_bit > common_bits) {
      while (true) {
        // why calculate id & rank as this ? see CritBitTrie::index
        uint64_t pos = id * 2 + rank_inc;
        id = encoded_trie_.rank1(pos);
        rank += (pos - id) - layer_rank_[layer];
        if (encoded_trie_.is1(pos)) {
          ++layer;
          ++id;
        } else {
          break;
        }
      }
      break;
    } else {
      uint64_t pos = id * 2 + path[layer].is_right;
      if (layer + 1 < path.size()) {
        assert(encoded_trie_.is1(pos));
        id = path[layer + 1].id;
        assert(id == encoded_trie_.rank1(pos + 1));
        // ∵ encoded_trie_.is1(pos)
        // ∴ encoded_trie_.rank1(pos) == encoded_trie_.rank1(pos + 1) - 1
        // ∴         (pos - id) - layer_rank_[layer] + 1
        //         = (pos - encoded_trie_.rank1(pos + 1)) - layer_rank_[layer] + 1
        //         = (pos - (encoded_trie_.rank1(pos) - 1)) - layer_rank_[layer] + 1
        //         = (pos - encoded_trie_.rank1(pos)) - layer_rank_[layer]
        //         = encoded_trie_.rank0(pos) - layer_rank_[layer]
        rank += (pos - id) - layer_rank_[layer] + 1;
        ++layer;
      } else {
        assert(encoded_trie_.is0(pos));
        id = encoded_trie_.rank1(pos);
        rank += (pos - id) - layer_rank_[layer];
        break;
      }
    }
  }
  while (id != layer_id_[layer++]) {
    uint64_t pos = (id + 1) * 2;
    id = encoded_trie_.rank1(pos);
    rank += (pos - id) - layer_rank_[layer];
  }
  return rank + rank_inc;
}

}  // namespace terark
