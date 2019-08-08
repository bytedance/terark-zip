#pragma once

#include <boost/predef/other/endian.h>
#include <boost/preprocessor/repeat.hpp>
#include <terark/fstring.hpp>
#include <terark/valvec.hpp>

#ifdef _MSC_VER
#  define ENTROPY_FORCE_INLINE __forceinline
#elif defined(__GNUC__) || defined(__INTEL_COMPILER)
#  define ENTROPY_FORCE_INLINE __attribute__((always_inline))
#else
#  define ENTROPY_FORCE_INLINE inline
#endif

#if !defined(BOOST_ENDIAN_BIG_BYTE) && !defined(BOOST_ENDIAN_LITTLE_BYTE)
#error must define byte endian
#endif

namespace terark {

class TerarkContext;

class ContextBuffer {
private:
    valvec<byte_t> b_;
    TerarkContext *c_;

    ContextBuffer(const ContextBuffer&) = delete;
    ContextBuffer& operator = (const ContextBuffer&) = delete;

public:
    ContextBuffer() : c_(nullptr) {}
    ContextBuffer(valvec<byte_t> &&b, TerarkContext* c) : b_(std::move(b)), c_(c) {
    }
    ContextBuffer(ContextBuffer&& other) : b_(std::move(other.b_)), c_(other.c_) {
        other.c_ = nullptr;
    }
    ContextBuffer& operator = (ContextBuffer&& other) {
        this->~ContextBuffer();
        ::new(this) ContextBuffer(std::move(other));
        return *this;
    }
    ~ContextBuffer();

    TerarkContext* owner() const { return c_; }

    operator valvec<byte_t>&() noexcept { return b_; }
    operator fstring() const noexcept { return b_; }
    valvec<byte_t>& get() noexcept { return b_; }

    byte_t* data() noexcept { return b_.data(); }
    size_t size() noexcept { return b_.size(); }
    size_t capacity() noexcept { return b_.capacity(); }

    void resize(size_t s, byte_t v = 0) { b_.resize(s, v); }
    void resize_no_init(size_t s) { b_.resize_no_init(s); }
    void ensure_capacity(size_t cap) { b_.ensure_capacity(cap); }
};

class TerarkContext {
private:
    friend class ContextBuffer;
    struct BufferList {
        BufferList* next;
        size_t c;
    };
    BufferList *list_ = nullptr;

    TerarkContext(const ContextBuffer&) = delete;
    TerarkContext(ContextBuffer&&) = delete;
    TerarkContext& operator = (const ContextBuffer&) = delete;
    TerarkContext& operator = (ContextBuffer&&) = delete;
public:
    TerarkContext() = default;
    ~TerarkContext() {
        while (list_ != nullptr) {
            auto l = list_->next;
            valvec<byte_t>().risk_set_data(reinterpret_cast<byte_t*>(list_), list_->c);
            list_ = l;
        }
    }

    ContextBuffer alloc(size_t size = 0);
};

class TerarkContext* GetTlsTerarkContext();

struct EntropyBytes {
    fstring data;
    ContextBuffer buffer;
};
struct EntropyBits {
    byte_t* data;
    size_t skip;
    size_t size;
    ContextBuffer buffer;
};

class EntropyBitsReader {
public:
    ENTROPY_FORCE_INLINE EntropyBitsReader(const EntropyBits& bits) {
        data_ = bits.data + bits.skip / 8;
        size_ = bits.size;
        size_t s = bits.skip % 8;
        cache_ = 0;
        if (size_ > 0) {
            size_t c = ((s + size_ + 7) / 8 + 7) % 8 + 1;
            memcpy((byte_t*)&cache_ + (8 - c), data_, c);
            data_ += c;
            remain_ = c * 8 - s;
        } else {
            remain_ = 0;
        }
    }
    ENTROPY_FORCE_INLINE void read(size_t bit_count, uint64_t* pbits, size_t* pshift) {
        static constexpr uint64_t mask[] = {
#define MAKE_MASK(z, n, u) n == 64 ? uint64_t(-1) : (~(uint64_t(-1) >> (n & 63))),
            BOOST_PP_REPEAT(65, MAKE_MASK, ~)
#undef MAKE_MASK
        };
        assert(bit_count <= size_);
        assert(bit_count + *pshift <= 64);
        assert(bit_count > 0);
        size_ -= bit_count;
        if (terark_likely(bit_count <= remain_)) {
            remain_ -= bit_count;
            *pbits |= ((cache_ << remain_) & mask[bit_count]) >> *pshift;
        }
        else {
            uint64_t bits = remain_ > 0 ? (cache_ >> (bit_count - remain_)) : 0;
            cache_ = (uint64_t&)*data_;
            remain_ += 64 - bit_count;
            data_ += 8;
            *pbits |= ((bits | (cache_ << remain_)) & mask[bit_count]) >> *pshift;
        }
        *pshift += bit_count;
    }
    ENTROPY_FORCE_INLINE void skip(size_t bit_count) {
        // |                                         | <- data
        // |-----------------------------------------|
        // |                            |<- remain ->|
        // |                            |<-              bit_count              ->|<-  used  ->|
        // |                                         | <-             uint64 ptr            -> |
        assert(bit_count <= size_);
        size_ -= bit_count;
        uint64_t bit_end = (uint64_t)data_ * 8 - remain_ + bit_count;
        uint64_t bit_align = (uint64_t)data_ % 8 * 8;
        remain_ = (~(bit_end - bit_align) + 1) % 64;
        data_ = (byte_t*)((bit_end + remain_) / 8);
        cache_ = *((uint64_t*)data_ - 1);
    }
    ENTROPY_FORCE_INLINE size_t size() {
        return size_;
    }

public:
    byte_t* data_;
    size_t size_;
    uint64_t cache_;
    size_t remain_;
};

template<class Buffer>
class EntropyBitsReverseWriter {
public:
    ENTROPY_FORCE_INLINE EntropyBitsReverseWriter(Buffer* b) {
        buffer_ = b;
        reset();
    }
    static void prepare(byte_t** pptr, size_t size, Buffer* buffer) {
        *pptr -= size;
        if (*pptr < buffer->data()) {
            size_t pos = *pptr + size - buffer->data();
            size_t len = buffer->capacity() - pos;
            buffer->ensure_capacity(buffer->capacity() + size);
            memmove(*pptr = buffer->data() + buffer->capacity() - len, buffer->data() + pos, len);
            *pptr -= size;
        }
    }
    ENTROPY_FORCE_INLINE void write(uint64_t bits, size_t bit_count) {
        static constexpr uint64_t mask[] = {
#define MAKE_MASK(z, n, u) n == 64 ? uint64_t(-1) : (~(uint64_t(-1) >> (n & 63))),
            BOOST_PP_REPEAT(65, MAKE_MASK, ~)
#undef MAKE_MASK
        };
        assert(bit_count > 0);
        assert(bit_count <= 64);
        bits &= mask[bit_count];
        cache_ |= bits >> written_;
        written_ += bit_count;
        if (terark_unlikely(written_ >= 64)) {
            prepare(&ptr_, 8, buffer_);
            (uint64_t&)*ptr_ = cache_;
            written_ -= 64;
            cache_ = bits << (bit_count - written_);
        }
    }
    ENTROPY_FORCE_INLINE EntropyBits finish(ContextBuffer* ctx_buffer) {
        size_t c = (written_ + 7) / 8;
        prepare(&ptr_, c, buffer_);
        memcpy(ptr_, (byte_t*)&cache_ + (8 - c), c);
        size_t skip_ = c * 8 - written_;
        return EntropyBits{
            ptr_, skip_,
            (buffer_->data() + buffer_->capacity() - ptr_) * 8 - skip_,
            ctx_buffer == nullptr ? ContextBuffer() : std::move(*ctx_buffer)
        };
    }
    ENTROPY_FORCE_INLINE void reset() {
        written_ = 0;
        cache_ = 0;
        ptr_ = buffer_->data() + buffer_->capacity();
    }

private:
    Buffer* buffer_;
    uint64_t cache_;
    size_t written_;
    byte_t* ptr_;
};

template<class Output>
class EntropyBitsWriter {
public:
    ENTROPY_FORCE_INLINE EntropyBitsWriter(Output& o) : output_(o) {
        reset();
    }
    ENTROPY_FORCE_INLINE void write(const EntropyBits& bits) {
        EntropyBitsReader reader(bits);
        while (reader.size() >= remain_) {
            size_t bit_shift = 0;
            reader.read(remain_, &cache_, &bit_shift);
            output_(&cache_, 8);
            cache_ = 0;
            remain_ = 64;
            output_byte_ += 8;
        }
        if (reader.size() > 0) {
            size_t bit_shift = remain_ = remain_ - reader.size();
            reader.read(reader.size(), &cache_, &bit_shift);
        }
        if (remain_ == 0) {
            output_(&cache_, 8);
            cache_ = 0;
            remain_ = 64;
            output_byte_ += 8;
        }
    }
    ENTROPY_FORCE_INLINE EntropyBits finish() {
        size_t w = 64 - remain_;
        size_t c = (w + 7) / 8;
        output_((byte_t*)&cache_, c);
        output_byte_ += c;
        return { nullptr, 0, output_byte_ * 8 + w - c * 8, {} };
    }
    ENTROPY_FORCE_INLINE void reset() {
        remain_ = 64;
        cache_ = 0;
        output_byte_ = 0;
    }

private:
    Output& output_;
    uint64_t cache_;
    size_t remain_;
    size_t output_byte_;
};

EntropyBytes EntropyBitsToBytes(EntropyBits* bits);
EntropyBits EntropyBytesToBits(fstring bytes);

class TERARK_DLL_EXPORT freq_hist {
public:
    struct histogram_t {
        uint64_t o0_size;
        uint64_t o0[256];
    };

private:
    static constexpr size_t MAGIC = 8;
    histogram_t hist_;
    uint64_t h1[256 + MAGIC];
    uint64_t h2[256 + MAGIC];
    uint64_t h3[256 + MAGIC];
    size_t min_;
    size_t max_;

public:
    freq_hist(size_t min_len = 0, size_t max_len = size_t(-1));
    void clear();

    static void normalise_hist(uint64_t* h, uint64_t& size, size_t normalise);

    const histogram_t& histogram() const;
    static size_t estimate_size(const histogram_t& hist);

    void add_record(fstring sample);
    void finish();
    void normalise(size_t norm);
};

class TERARK_DLL_EXPORT freq_hist_o1 {
public:
    struct histogram_t : freq_hist::histogram_t {
        uint64_t o1_size[256];
        uint64_t o1[256][256];
    };

private:
    histogram_t hist_;
    uint8_t o1_[256][256];
    size_t min_;
    size_t max_;

public:
    freq_hist_o1(bool r1 = false, size_t min_len = 0, size_t max_len = size_t(-1));
    freq_hist_o1(const freq_hist_o1&) = default;
    freq_hist_o1& operator = (const freq_hist_o1&) = default;
    void clear();
    void reset1();

    const histogram_t& histogram() const;
    static size_t estimate_size(const histogram_t& hist);
    static size_t estimate_size_unfinish(const freq_hist_o1& freq);
    static size_t estimate_size_unfinish(const freq_hist_o1& freq0, const freq_hist_o1& freq1);
    void add_record(fstring sample);
    void add_hist(const freq_hist_o1& other);
    void finish();
    void normalise(size_t norm);
};

class TERARK_DLL_EXPORT freq_hist_o2 {
public:
    struct histogram_t : freq_hist_o1::histogram_t {
        uint64_t o2_size[256][256];
        uint64_t o2[256][256][256];
    };

private:
    histogram_t hist_;
    size_t min_;
    size_t max_;

public:
    freq_hist_o2(size_t min_len = 0, size_t max_len = size_t(-1));
    void clear();

    const histogram_t& histogram() const;
    static size_t estimate_size(const histogram_t& hist);
    static size_t estimate_size_unfinish(const histogram_t& hist);

    void add_record(fstring sample);
    void finish();
    void normalise(size_t norm);
};

// be careful with potential memory out of bounds
inline uint32_t load_uint32_from_bits(const void* data, size_t skip) {
    //  [  0  ][  1  ][  2  ][  3  ][  4  ][  5  ][  6  ][  7  ][  8  ][  9  ]
    //  |----------------------- loaded -----------------------| 64bits
    //     |----------- needed --------|
    //  |lo|                           |----------- hi --------| little endian
    //  |hi|                           |----------- lo --------| big    endian
    size_t q = skip / 8;
    size_t r = skip % 8;
    uint64_t cache = unaligned_load<uint64_t>(reinterpret_cast<const byte_t*>(data) + q);
#ifndef BOOST_ENDIAN_LITTLE_BYTE // big endian
    cache >>= (64 - (r + 32));
#else                            // little endian
    cache >>= r;
#endif
    return static_cast<uint32_t>(cache);
}

}
