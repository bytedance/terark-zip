#include "int_vector.hpp"
#include <string>
#include "io/FileStream.hpp"
#include "io/StreamBuffer.hpp"
#include "io/DataOutput.hpp"
#include "fstring.hpp"

namespace terark {

void UintVecMin0Base::push_back_slow_path(size_t val) {
    // 103/64 is a bit less than 1.618
    if (val > m_mask) {
        size_t num = m_size;
        UintVecMin0Base tmp(std::max(num+1, num*103/64), val);
        auto d = m_data.data();
        auto b = m_bits;
        if (b <= 58) {
            auto m = m_mask;
            for (size_t i = 0; i < num; ++i) {
                tmp.set_wire(i, UintVecMin0::fast_get(d, b, m, i));
            }
        }
        else {
            for (size_t i = 0; i < num; ++i) {
                tmp.set_wire(i, BigUintVecMin0::fast_get(d, b, i));
            }
        }
        tmp.m_size = num;
        this->swap(tmp);
    }
    else {
        m_data.resize(std::max(size_t(32), align_up(m_data.size() * 103/64, 16)));
    }
    TERARK_VERIFY_LT(compute_mem_size(m_bits, m_size+1), m_data.size());
    set_wire(m_size++, val);
}

UintVecMin0Base::Builder::~Builder() {
}

class UintVecMin0Builder : public UintVecMin0Base::Builder {
protected:
    boost::intrusive_ptr<OutputBuffer> m_writer;
    UintVecMin0Base m_buffer;
    size_t m_count;
    size_t m_flush_count;
    size_t m_output_size;

    static const size_t offset_flush_size = 128;
public:
    UintVecMin0Builder(size_t uintbits, OutputBuffer* buffer)
        : m_writer(buffer)
        , m_count(0)
        , m_flush_count(0)
        , m_output_size(0) {
        m_buffer.resize_with_uintbits(offset_flush_size, uintbits);
    }

    ~UintVecMin0Builder() {
    }

    void push_back(size_t value) override {
        m_buffer.set_wire(m_count++, value);
        if (m_count == offset_flush_size) {
            size_t byte_count = m_buffer.uintbits() * offset_flush_size / 8;
            m_writer->ensureWrite(m_buffer.data(), byte_count);
            m_flush_count += offset_flush_size;
            m_output_size += byte_count;
            m_count = 0;
        }
    }
    BuildResult finish() override {
        if (m_count > 0) {
            m_buffer.resize(m_count);
            m_buffer.shrink_to_fit();
            m_writer->ensureWrite(m_buffer.data(), m_buffer.mem_size());
            m_output_size += m_buffer.mem_size();
        }
        else {
            size_t remain_size = UintVecMin0Base::compute_mem_size(m_buffer.uintbits(), 0);
            m_output_size += remain_size;
            byte zero = 0;
            while (remain_size-- > 0) {
                m_writer->ensureWrite(&zero, 1);
            }
        }
        m_writer->flush_buffer();
        return {
            m_buffer.uintbits(),
            m_flush_count + m_count,
            m_output_size,
        };
    }
};

class UintVecMin0FileBuilder : public UintVecMin0Builder {
protected:
    FileStream m_file;
public:
    UintVecMin0FileBuilder(size_t uintbits, const char* fpath)
        : UintVecMin0Builder(uintbits, new OutputBuffer())
        , m_file(fpath, "wb+") {
        m_file.disbuf();
        UintVecMin0Builder::m_writer->attach(&m_file);
    }
    BuildResult finish() override {
        auto result = UintVecMin0Builder::finish();
        m_file.close();
        return result;
    }
    ~UintVecMin0FileBuilder() {
        UintVecMin0Builder::m_writer.reset();
    }
};


UintVecMin0Base::Builder*
UintVecMin0Base::create_builder_by_uintbits(size_t uintbits, const char* fpath) {
    return new UintVecMin0FileBuilder(uintbits, fpath);
}

UintVecMin0Base::Builder*
UintVecMin0Base::create_builder_by_max_value(size_t max_val, const char* fpath) {
    return new UintVecMin0FileBuilder(UintVecMin0::compute_uintbits(max_val), fpath);
}

UintVecMin0Base::Builder*
UintVecMin0Base::create_builder_by_uintbits(size_t uintbits, OutputBuffer* buffer) {
    return new UintVecMin0Builder(uintbits, buffer);
}

UintVecMin0Base::Builder*
UintVecMin0Base::create_builder_by_max_value(size_t max_val, OutputBuffer* buffer) {
    return new UintVecMin0Builder(UintVecMin0::compute_uintbits(max_val), buffer);
}

terark_flatten
std::pair<size_t, size_t>
UintVecMin0::equal_range(size_t lo, size_t hi, size_t key) const noexcept {
    assert(lo <= hi);
    assert(hi <= m_size);
    size_t bits = m_bits;
    size_t mask = m_mask;
    const byte_t* data = m_data.data();
    while (lo < hi) {
        size_t mid_idx = (lo + hi) / 2;
        size_t bit_idx = bits * mid_idx;
        size_t mid_val = unaligned_load<size_t>(data + bit_idx / 8);
        mid_val = (mid_val >> bit_idx % 8) & mask;
        if (mid_val < key)
            lo = mid_idx + 1;
        else if (key < mid_val)
            hi = mid_idx;
        else
            return std::make_pair(lower_bound(lo, mid_idx, key),
                                  upper_bound(mid_idx+1, hi, key));
    }
    return std::make_pair(lo, lo);
}

size_t
UintVecMin0::lower_bound(size_t lo, size_t hi, size_t key) const noexcept {
    assert(lo <= hi);
    assert(hi <= m_size);
    size_t bits = m_bits;
    size_t mask = m_mask;
    const byte_t* data = m_data.data();
    while (lo < hi) {
        size_t mid_idx = (lo + hi) / 2;
        size_t bit_idx = bits * mid_idx;
        size_t mid_val = unaligned_load<size_t>(data + bit_idx / 8);
        mid_val = (mid_val >> bit_idx % 8) & mask;
        if (mid_val < key)
            lo = mid_idx + 1;
        else
            hi = mid_idx;
    }
    return lo;
}

size_t
UintVecMin0::upper_bound(size_t lo, size_t hi, size_t key) const noexcept {
    assert(lo <= hi);
    assert(hi <= m_size);
    size_t bits = m_bits;
    size_t mask = m_mask;
    const byte_t* data = m_data.data();
    while (lo < hi) {
        size_t mid_idx = (lo + hi) / 2;
        size_t bit_idx = bits * mid_idx;
        size_t mid_val = unaligned_load<size_t>(data + bit_idx / 8);
        mid_val = (mid_val >> bit_idx % 8) & mask;
        if (mid_val <= key)
            lo = mid_idx + 1;
        else
            hi = mid_idx;
    }
    return lo;
}

terark_flatten
std::pair<size_t, size_t>
BigUintVecMin0::equal_range(size_t lo, size_t hi, size_t key) const noexcept {
    assert(lo <= hi);
    assert(hi <= m_size);
    size_t bits = m_bits;
    const byte_t* data = m_data.data();
    while (lo < hi) {
        size_t mid_idx = (lo + hi) / 2;
        size_t mid_val = fast_get(data, bits, mid_idx);
        if (mid_val < key)
            lo = mid_idx + 1;
        else if (key < mid_val)
            hi = mid_idx;
        else
            return std::make_pair(lower_bound(lo, mid_idx, key),
                                  upper_bound(mid_idx+1, hi, key));
    }
    return std::make_pair(lo, lo);
}

size_t
BigUintVecMin0::lower_bound(size_t lo, size_t hi, size_t key) const noexcept {
    assert(lo <= hi);
    assert(hi <= m_size);
    size_t bits = m_bits;
    const byte_t* data = m_data.data();
    while (lo < hi) {
        size_t mid_idx = (lo + hi) / 2;
        size_t mid_val = fast_get(data, bits, mid_idx);
        if (mid_val < key)
            lo = mid_idx + 1;
        else
            hi = mid_idx;
    }
    return lo;
}

size_t
BigUintVecMin0::upper_bound(size_t lo, size_t hi, size_t key) const noexcept {
    assert(lo <= hi);
    assert(hi <= m_size);
    size_t bits = m_bits;
    const byte_t* data = m_data.data();
    while (lo < hi) {
        size_t mid_idx = (lo + hi) / 2;
        size_t mid_val = fast_get(data, bits, mid_idx);
        if (mid_val <= key)
            lo = mid_idx + 1;
        else
            hi = mid_idx;
    }
    return lo;
}

} // namespace terark

