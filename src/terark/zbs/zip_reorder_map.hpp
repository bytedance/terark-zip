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
    terark::MmapWholeFile file_;
    const byte_t *pos_ = nullptr;
    size_t current_value_ = -1;
    size_t seq_length_ = 0;
    size_t size_ = 0;
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
            if (pos_ > (const byte_t*)file_.base + file_.size) {
                THROW_STD(out_of_range, "ZReorderMap read seq out of range");
            }
        }
        current_value_ >>= 1;
    }
  public:
    bool eof() const {
        assert(i_ <= size_);
        return i_ == size_;
    }
    size_t size() const {
        return size_;
    }
    size_t index() const {
        assert(i_ < size_);
        return i_;
    }
    size_t operator*() const {
        return current_value_;
    }
    ZReorderMap& operator++() {
        assert(i_ < size_);
        assert(seq_length_ > 0);
        ++i_;
        (intptr_t&)current_value_ += sign_;
        if (--seq_length_ == 0 && i_ < size_) {
            read_();
        }
        return *this;
    }
    void rewind() {
        if (file_.size < 16) {
            THROW_STD(out_of_range, "ZReorderMap rewind out_of_range");
        }
        pos_ = (const byte_t*)file_.base;
        memcpy(&size_, pos_, sizeof(size_t));
        pos_ += sizeof(size_t);
        memcpy(&sign_, pos_, sizeof(ptrdiff_t));
        pos_ += sizeof(ptrdiff_t);
        i_ = 0;
        if (i_ < size_) {
            read_();
        }
    }
    template<class ...args_t>
    ZReorderMap(args_t&& ...args)
        : file_(std::forward<args_t>(args)...) {
        rewind();
    }

    class TERARK_DLL_EXPORT Builder {
      private:
        FileStream file_;
        NativeDataOutput<OutputBuffer> writer_;
        size_t base_value_;
        size_t seq_length_;
        intptr_t sign_;
        size_t size_;
        public:
        template<class ...args_t>
        Builder(size_t size, int sign, args_t&& ...args)
            : file_(std::forward<args_t>(args)...)
            , writer_(&file_)
            , base_value_(-1)
            , seq_length_(0)
            , sign_(sign)
            , size_(size) {
            assert(sign == 1 || sign == -1);
            file_.disbuf();
            writer_ << size << sign_;
        }
        void push_back(size_t value);
        void finish();
    };
};

} // namespace terark

#endif // __terark_zip_reorder_map_hpp__