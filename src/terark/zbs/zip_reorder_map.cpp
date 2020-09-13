
#include "zip_reorder_map.hpp"

namespace terark {


ZReorderMap::Builder::~Builder() {
}

void ZReorderMap::Builder::push_back(size_t value) {
    assert(size_ > 0);
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
    --size_;
}

void ZReorderMap::Builder::finish() {
    assert(size_ == 0);
    if (seq_length_ > 1) {
        size_t current_value = base_value_ << 1;
        writer_.ensureWrite(&current_value, 5);
        writer_ << var_uint64_t(seq_length_);
    }
    else if (seq_length_ == 1) {
        size_t current_value = (base_value_ << 1) | 1;
        writer_.ensureWrite(&current_value, 5);
    }
    writer_.flush_buffer();
    file_.flush();
}

} // namespace terark
