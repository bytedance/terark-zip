
#include "zip_reorder_map.hpp"

namespace terark {


void ZReorderMap::Reader::read_slice() {
  if (file_.size < sizeof(size_t) * 2) {
    THROW_STD(out_of_range, "ZReorderMap rewind out_of_range");
  }
  size_t vector_size;
  auto pos = (const byte_t*)file_.base + file_.size;
  memcpy(&vector_size, pos -= sizeof(size_t), sizeof(size_t));
  memcpy(&sign_, pos -= sizeof(size_t), sizeof(size_t));
  if (file_.size < (vector_size * 4 + 2) * sizeof(size_t)) {
    THROW_STD(out_of_range, "ZReorderMap rewind out_of_range");
  }
  slice_vec_.resize(vector_size);
  for (size_t i = vector_size; i-- > 0; ) {
    memcpy(&slice_vec_[i].size, pos -= sizeof(size_t), sizeof(size_t));
    memcpy(&slice_vec_[i].base, pos -= sizeof(size_t), sizeof(size_t));
    memcpy(&slice_vec_[i].length, pos -= sizeof(size_t), sizeof(size_t));
    memcpy(&slice_vec_[i].offset, pos -= sizeof(size_t), sizeof(size_t));
  }
}

void ZReorderMap::Builder::push_back(size_t value) {
  assert(size_ < total_size_);
  assert(value <= 0x7FFFFFFFFFULL);
  size_t next_value = size_t(intptr_t(base_value_) + intptr_t(seq_length_) * sign_);
  if (value != next_value) {
    if (seq_length_ > 1) {
      size_t current_value = base_value_ << 1;
      writer_.ensureWrite(&current_value, 5);
      writer_ << var_uint64_t(seq_length_);
    }
    else if (seq_length_ == 1) {
      size_t current_value = (base_value_ << 1) | 1;
      writer_.ensureWrite(&current_value, 5);
    }
    base_value_ = value;
    seq_length_ = 1;
  }
  else {
    ++seq_length_;
  }
  ++size_;
}

void ZReorderMap::Builder::push_slice() {
  if (seq_length_ > 1) {
    size_t current_value = base_value_ << 1;
    writer_.ensureWrite(&current_value, 5);
    writer_ << var_uint64_t(seq_length_);
  }
  else if (seq_length_ == 1) {
    size_t current_value = (base_value_ << 1) | 1;
    writer_.ensureWrite(&current_value, 5);
  }
  base_value_ = size_t(-1);
  seq_length_ = 0;
  size_t offset = file_.fsize() + writer_.bufpos();
  slice_vec_.emplace_back(Slice{
    last_offset_,
    offset - last_offset_,
    last_size_,
    size_ - last_size_,
  });
  last_offset_ = offset;
  last_size_ = size_;
}

void ZReorderMap::Builder::finish() {
  assert(last_size_ == total_size_);
  for (auto& slice : slice_vec_) {
    writer_ << slice.offset << slice.length << slice.base << slice.size;
  }
  writer_ << sign_;
  writer_ << slice_vec_.size();
  writer_.flush();
  file_.close();
}

} // namespace terark

