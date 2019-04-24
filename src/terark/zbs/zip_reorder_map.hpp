#ifndef __terark_zip_reorder_map_hpp__
#define __terark_zip_reorder_map_hpp__

#include <terark/io/DataOutput.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/var_int_inline.hpp>
#include <terark/util/mmap.hpp>

namespace terark {

class TERARK_DLL_EXPORT ZReorderMap {
private:
  struct Slice {
    size_t offset;
    size_t length;
    size_t base;
    size_t size;
  };

  const terark::MmapWholeFile &file_;
  Slice slice_;
  const byte_t *pos_ = nullptr;
  const byte_t *end_ = nullptr;
  size_t current_value_ = -1;
  size_t seq_length_ = 0;
  size_t i_ = 0;
  intptr_t sign_ = 0;
private:
  void read_() {
    current_value_ = 0;
    if (pos_ + 5 > (const byte_t*)file_.base + file_.size) {
      THROW_STD(out_of_range, "ZReorderMap read value out of range");
    }
    memcpy(&current_value_, pos_, 5);
    pos_ += 5;
    if (current_value_ & 1) {
      seq_length_ = 1;
    }
    else {
      seq_length_ = gg_load_var_uint<size_t>(pos_, &pos_, BOOST_CURRENT_FUNCTION);
      if (pos_ > end_) {
        THROW_STD(out_of_range, "ZReorderMap read seq out of range");
      }
    }
    current_value_ >>= 1;
  }
public:
  bool eof() const {
    assert(i_ <= slice_.size);
    return i_ == slice_.size;
  }
  size_t size() const {
    return slice_.size;
  }
  size_t index() const {
    assert(i_ < slice_.size);
    return i_;
  }
  size_t operator*() const {
    assert(current_value_ >= slice_.base);
    assert(current_value_ < slice_.base + slice_.size);
    return current_value_ - slice_.base;
  }
  ZReorderMap& operator++() {
    assert(i_ < slice_.size);
    assert(seq_length_ > 0);
    ++i_;
    (intptr_t&)current_value_ += sign_;
    if (--seq_length_ == 0 && i_ < slice_.size) {
      read_();
    }
    return *this;
  }
  void rewind() {
    pos_ = (const byte_t*)file_.base + slice_.offset;
    end_ = pos_ + slice_.length;
    i_ = 0;
    if (i_ < slice_.size) {
      read_();
    }
  }
  ZReorderMap(const terark::MmapWholeFile& file, Slice slice, intptr_t sign)
    : file_(file)
    , slice_(slice)
    , sign_(sign) {
    rewind();
  }

public:
  class TERARK_DLL_EXPORT Reader {
  private:
    terark::MmapWholeFile file_;
    std::vector<Slice> slice_vec_;
    intptr_t sign_ = 0;
  public:
    ZReorderMap get_global() const {
      Slice global = {
        0, 0, 0, 0
      };
      for (auto& s : slice_vec_) {
        global.length += s.length;
        global.size += s.size;
      }
      return ZReorderMap(file_, global, sign_);
    }
    ZReorderMap get_slice(size_t i) const {
      return ZReorderMap(file_, slice_vec_[i], sign_);
    }
    size_t slice_count() const {
      return slice_vec_.size();
    }
    void read_slice();

    template<class ...args_t>
    Reader(args_t&& ...args)
      : file_(std::forward<args_t>(args)...) {
      read_slice();
    }
  };

  class TERARK_DLL_EXPORT Builder {
  private:
    FileStream file_;
    NativeDataOutput<OutputBuffer> writer_;
    std::vector<Slice> slice_vec_;
    size_t last_offset_;
    size_t last_size_;
    size_t size_;
    size_t base_value_;
    size_t seq_length_;
    intptr_t sign_;
    size_t total_size_;
  public:
    template<class ...args_t>
    Builder(size_t size, int sign, args_t&& ...args)
      : file_(std::forward<args_t>(args)...)
      , writer_(&file_)
      , last_offset_(0)
      , last_size_(0)
      , size_(0)
      , base_value_(-1)
      , seq_length_(0)
      , sign_(sign)
      , total_size_(size) {
      assert(sign == 1 || sign == -1);
      file_.disbuf();
      //slice_vec_.emplace_back(Slice{});
      //writer_ << size << sign_;
    }
    void push_back(size_t value);
    void push_slice();
    void finish();
  };
};

} // namespace terark

#endif // __terark_zip_reorder_map_hpp__
