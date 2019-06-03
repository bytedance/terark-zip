#include "rans_encoding.hpp"
#include <memory>
#include <cmath>
#include <terark/bitmanip.hpp>

// --------------------------------------------------------------------------

namespace terark { namespace rANS_static_64 {

// L ('l' in the paper) is the lower bound of our normalization interval.
// Between this and our 32-bit-aligned emission, we use 63 (not 64!) bits.
// This is done intentionally because exact reciprocals for 63-bit uints
// fit in 64-bit uints: this permits some optimizations during encoding.
static constexpr size_t RANS_L_BITS = 16;
static constexpr size_t RANS_L = (1ull << RANS_L_BITS); // lower bound of our normalization interval

static constexpr size_t BLOCK_SIZE = 4;                 // bytes pre write or read

static constexpr size_t HEAD_BITS = 9;                  // (head size - 2) + size * 7
static constexpr size_t RECORD_MAX_SIZE = (1 << HEAD_BITS) / 7;

template<size_t a, size_t n>
struct pow_t {
    static constexpr size_t value = (n % 2 ? a : 1) * pow_t<a, n / 2>::value * pow_t<a, n / 2>::value;
};
template<size_t a>
struct pow_t<a, 1> {
    static constexpr size_t value = a;
};
template<size_t a>
struct pow_t<a, 0> {
    static constexpr size_t value = 1;
};

typedef uint64_t Rans64State;

// This code needs support for 64-bit long multiplies with 128-bit result
// (or more precisely, the top 64 bits of a 128-bit result). This is not
// really portable functionality, so we need some compiler-specific hacks
// here.

#if defined(_MSC_VER) && (ULONG_MAX) > (UINT_MAX)

#include <intrin.h>

static inline uint64_t Rans64MulHi(uint64_t a, uint64_t b) {
    return __umulh(a, b);
}

#elif defined(__GNUC__) && (ULONG_MAX) > (UINT_MAX)

static inline uint64_t Rans64MulHi(uint64_t a, uint64_t b) {
    return (uint64_t)(((unsigned __int128)a * b) >> 64);
}

#else

static inline uint64_t Rans64MulHi(uint64_t x, uint64_t y) {
#define LO(x) ((x)&0xffffffff)
#define HI(x) ((x)>>32)

    uint64_t A = HI(x), B = LO(x);
    uint64_t C = HI(y), D = LO(y);
    uint64_t X0 = D * B;
    uint64_t X1 = LO(D*A) + LO(C*B) + HI(X0);
    uint64_t X2 = HI(D*A) + HI(C*B) + LO(C*A) + HI(X1);
    uint64_t X3 = HI(C*A) + HI(X2);

    // X3, LO(X2), LO(X1), LO(X0)
    return (X3 << 32) + LO(X2);

#undef LO
#undef HI
}
#endif

// --------------------------------------------------------------------------

// Initializes an encoder symbol to start "start" and frequency "freq"
static inline void Rans64EncSymbolInit(Rans64EncSymbol* s, uint32_t start, uint32_t freq, uint32_t scale_bits) {
    assert(scale_bits <= 31);
    assert(start <= (1u << scale_bits));
    assert(freq <= (1u << scale_bits) - start);

    // Say M := 1 << scale_bits.
    //
    // The original encoder does:
    //   x_new = (x/freq)*M + start + (x%freq)
    //
    // The fast encoder does (schematically):
    //   q     = mul_hi(x, rcp_freq) >> rcp_shift   (division)
    //   r     = x - q*freq                         (remainder)
    //   x_new = q*M + bias + r                     (new x)
    // plugging in r into x_new yields:
    //   x_new = bias + x + q*(M - freq)
    //        =: bias + x + q*cmpl_freq             (*)
    //
    // and we can just precompute cmpl_freq. Now we just need to
    // set up our parameters such that the original encoder and
    // the fast encoder agree.

    s->freq = freq;
    s->cmpl_freq = ((1 << scale_bits) - freq);
    if (freq < 2) {
        // freq=0 symbols are never valid to encode, so it doesn't matter what
        // we set our values to.
        //
        // freq=1 is tricky, since the reciprocal of 1 is 1; unfortunately,
        // our fixed-point reciprocal approximation can only multiply by values
        // smaller than 1.
        //
        // So we use the "next best thing": rcp_freq=~0, rcp_shift=0.
        // This gives:
        //   q = mul_hi(x, rcp_freq) >> rcp_shift
        //     = mul_hi(x, (1<<64) - 1)) >> 0
        //     = floor(x - x/(2^64))
        //     = x - 1 if 1 <= x < 2^64
        // and we know that x>0 (x=0 is never in a valid normalization interval).
        //
        // So we now need to choose the other parameters such that
        //   x_new = x*M + start
        // plug it in:
        //     x*M + start                   (desired result)
        //   = bias + x + q*cmpl_freq        (*)
        //   = bias + x + (x - 1)*(M - 1)    (plug in q=x-1, cmpl_freq)
        //   = bias + 1 + (x - 1)*M
        //   = x*M + (bias + 1 - M)
        //
        // so we have start = bias + 1 - M, or equivalently
        //   bias = start + M - 1.
        s->rcp_freq = ~0ull;
        s->rcp_shift = 0;
        s->bias = start + (1 << scale_bits) - 1;
    }
    else {
        // Alverson, "Integer Division using reciprocals"
        // shift=ceil(log2(freq))
        uint32_t shift = 0;
        uint64_t x0, x1, t0, t1;
        while (freq > (1u << shift))
            shift++;

        // long divide ((uint128) (1 << (shift + 63)) + freq-1) / freq
        // by splitting it into two 64:64 bit divides (this works because
        // the dividend has a simple form.)
        x0 = freq - 1;
        x1 = 1ull << (shift + 31);

        t1 = x1 / freq;
        x0 += (x1 % freq) << 32;
        t0 = x0 / freq;

        s->rcp_freq = t0 + (t1 << 32);
        s->rcp_shift = shift - 1;

        // With these values, 'q' is the correct quotient, so we
        // have bias=start.
        s->bias = start;
    }
}

// Initialize a rANS encoder.
static inline void Rans64EncInit(Rans64State* r, const byte_t** pptr, size_t* psize) {
    uint64_t x = 1;
    while (x < RANS_L && *psize > 0) {
        *pptr -= 1;
        *psize -= 1;
        x = (x << 8) | **pptr;
    }
    *r = x;
}

// Encodes a given symbol. This is faster than straight RansEnc since we can do
// multiplications instead of a divide.
//
// See RansEncSymbolInit for a description of how this works.
static inline void Rans64EncPutSymbol(Rans64State* r, const Rans64EncSymbol* sym, uint32_t scale_bits) {
    assert(sym->freq != 0); // can't encode symbol with freq=0
    uint64_t x = *r;

    // x = C(s,x)
    uint64_t q = Rans64MulHi(x, sym->rcp_freq) >> sym->rcp_shift;
    *r = x + sym->bias + q * sym->cmpl_freq;
}

// Ensure context buffer capacity
static inline void Rans64EncWrite(byte_t** pptr, const void* data, size_t size, ContextBuffer& buffer) {
    *pptr -= size;
    if (*pptr < buffer.data()) {
        size_t pos = *pptr + size - buffer.data();
        size_t len = buffer.size() - pos;
        buffer.resize_no_init(buffer.size() * 2);
        memmove(*pptr = buffer.data() + buffer.size() - len, buffer.data() + pos, len);
        *pptr -= size;
    }
    memcpy(*pptr, data, size);
}

// Renormalize.
static inline void Rans64EncRenorm(Rans64State* r, byte_t** pptr, ContextBuffer& buffer, size_t freq, uint32_t scale_bits) {
    assert(freq > 0);
    // renormalize
    uint64_t x = *r;
    uint64_t x_max = ((RANS_L >> scale_bits) << (BLOCK_SIZE * 8)) * freq; // turns into a shift

    if (x >= x_max) {
        Rans64EncWrite(pptr, &x, BLOCK_SIZE, buffer);
        x >>= (BLOCK_SIZE * 8);
        *r = x;
    }
}

// Flushes the rANS encoder.
static inline void Rans64EncFlush(Rans64State* r, byte_t** pptr, ContextBuffer& buffer, size_t* psize, XCheck check) {
    uint64_t x = *r;
    size_t c = (terark_bsr_u64(x) + 8 + HEAD_BITS + check.enable) / 8;
    assert(c >= 2 && c <= 8);
    size_t s = *psize % RECORD_MAX_SIZE * 7 + (c - 2);
    x = (x << HEAD_BITS) | s;
    if (check.enable) {
        x = (x << 1) | check.value;
    }
    *psize /= RECORD_MAX_SIZE;
    Rans64EncWrite(pptr, &x, c, buffer);
}

// Initialize a decoder symbol to start "start" and frequency "freq"
static inline void Rans64DecSymbolInit(Rans64DecSymbol* s, uint32_t start, uint32_t freq) {
    assert(start <= TOTFREQ);
    assert(freq <= TOTFREQ - start);
    s->start = start;
    s->freq = freq;
}

// Initializes a rANS decoder.
// Unlike the encoder, the decoder works forwards as you'd expect.
static inline bool Rans64DecInit(Rans64State* r, size_t* psize, const byte_t** pptr, const byte_t* end, XCheck check) {
    uint64_t x = 0;

    size_t s = (*(uint16_t*)*pptr & ((1ull << (HEAD_BITS + check.enable)) - 1));
    if (check.enable) {
        if ((s & 1) != check.value) {
            return false;
        }
        s >>= 1;
    }
    size_t c = s % 7 + 2;
    if (*pptr + c > end) return false;

    memcpy(&x, *pptr, c);
    *pptr += c;

    *psize = *psize * RECORD_MAX_SIZE + s / 7;
    *r = x >> (HEAD_BITS + check.enable);
    return true;
}

// Returns the current cumulative frequency (map it to a symbol yourself!)
static inline uint32_t Rans64DecGet(Rans64State* r, uint32_t scale_bits) {
    return *r & ((1u << scale_bits) - 1);
}

// Advances in the bit stream by "popping" a single symbol with range start
// "start" and frequency "freq". All frequencies are assumed to sum to "1 << scale_bits".
// No renormalization or output happens.
static inline void Rans64DecAdvanceStep(Rans64State* r, const Rans64DecSymbol* sym, uint32_t scale_bits) {
    uint64_t mask = (1u << scale_bits) - 1;

    // s, x = D(x)
    uint64_t x = *r;
    *r = sym->freq * (x >> scale_bits) + (x & mask) - sym->start;
}

// Renormalize.
static inline bool Rans64DecRenorm(Rans64State* r, const byte_t** pptr, const byte_t* end) {
    // renormalize
    uint64_t x = *r;
    if (x < RANS_L) {
        if (*pptr >= end) {
            return false;
        }
        x <<= (BLOCK_SIZE * 8);
        memcpy(&x, *pptr, BLOCK_SIZE);
        *pptr += BLOCK_SIZE;
        assert(x >= RANS_L);
    }
    *r = x;
    return true;
}

// Tail bytes.
static inline bool Rans64DecTail(Rans64State* r, byte_t** pptr, byte_t* end) {
    uint64_t x = *r;
    while (x > 1 && *pptr != end) {
        **pptr = x & 0xFF;
        *pptr += 1;
        x >>= 8;
    }
    return x == 1;
}

// build CTable
static inline void Rans64BuildCTable(byte_t** pptr, const uint64_t* freq, Rans64EncSymbol* syms) {
    size_t rle_j, j, x = 0;
    byte_t *&cp = *pptr;
    if (std::find_if(freq, freq + 256, [](uint64_t v) { return v > 0; }) == freq + 256) {
        *cp++ = 0;  // ignore
    }
    else {
        *cp++ = 1;  // ignore
        for (rle_j = j = 0; j < 256; j++) {
            if (freq[j]) {
                if (rle_j) {
                    rle_j--;
                }
                else {
                    *cp++ = j;
                    if (!rle_j && j && freq[j - 1]) {
                        for (rle_j = j + 1; rle_j < 256 && freq[rle_j]; rle_j++)
                            ;
                        rle_j -= j + 1;
                        *cp++ = rle_j;
                    }
                }

                if (freq[j] < 128) {
                    *cp++ = freq[j];
                }
                else {
                    *cp++ = 128 | (freq[j] >> 8);
                    *cp++ = freq[j] & 0xff;
                }

                Rans64EncSymbolInit(&syms[j], x, freq[j], TF_SHIFT);
                x += freq[j];
            }
        }
        *cp++ = 0;  // end
    }
}

// build DTable
static inline void Rans64BuildDTable(const byte_t** pptr, byte_t* ari, Rans64DecSymbol* syms) {
    size_t j, rle_j = 0, x = 0;
    const byte_t *&cp = *pptr;
    if (*cp++ == 0) return;
    j = *cp++;
    do {
        size_t F, C;
        if ((F = *cp++) >= 128) {
            F &= ~128;
            F = ((F & 127) << 8) | *cp++;
        }
        C = x;

        if (!F)
            F = TOTFREQ;

        Rans64DecSymbolInit(&syms[j], C, F);

        /* Build reverse lookup table */
        memset(&ari[x], j, F);
        x += F;

        assert(x <= TOTFREQ);

        if (!rle_j && j + 1 == *cp) {
            j = *cp++;
            rle_j = *cp++;
        }
        else if (rle_j) {
            rle_j--;
            j++;
        }
        else {
            j = *cp++;
        }
    } while (j);
}

// --------------------------------------------------------------------------

EntropyBytes encode(fstring record, TerarkContext* context) {
    freq_hist hist;
    hist.add_record(record);
    hist.finish();
    hist.normalise(TOTFREQ);
    encoder e(hist.histogram());
    auto ret_bytes = e.encode(record, context);
    auto ret = ret_bytes.data;
    auto buffer = context->alloc();
    assert(ret.udata() + ret.size() == buffer.data() + buffer.size());
    size_t table_size = e.table().size();
    if (buffer.data() + table_size > ret.udata()) {
        size_t pos = ret.udata() - buffer.data();
        buffer.resize_no_init(table_size + ret.size());
        memmove(buffer.data() + table_size, buffer.data() + pos, ret.size());
        memcpy(buffer.data(), e.table().data(), table_size);
        return {buffer, std::move(buffer)};
    }
    else {
        memcpy((byte_t*)ret.data() - table_size, e.table().data(), table_size);
        return {
            fstring(ret.data() - table_size, ret.size() + table_size),
            std::move(ret_bytes.buffer)
        };
    }
}

size_t decode(fstring data, valvec<byte_t>* record, TerarkContext* context) {
    size_t table_size;
    decoder d(data, &table_size);
    size_t read = d.decode(data.substr(table_size), record, context);
    return read == 0 ? 0 : table_size + read;
}

EntropyBytes encode_o1(fstring record, TerarkContext* context) {
    struct encoder_mem {
        freq_hist_o1 hist;
        encoder_o1 e;
    };
    std::unique_ptr<encoder_mem> p(new encoder_mem);
    p->hist.add_record(record);
    p->hist.finish();
    p->hist.normalise(TOTFREQ);
    p->e.init(p->hist.histogram());
    auto ret_bytes = p->e.encode(record, context);
    auto ret = ret_bytes.data;
    auto buffer = context->alloc();
    assert(ret.udata() + ret.size() == buffer.data() + buffer.size());
    size_t table_size = p->e.table().size();
    if (buffer.data() + table_size > ret.udata()) {
        size_t pos = ret.udata() - buffer.data();
        buffer.resize_no_init(table_size + ret.size());
        memmove(buffer.data() + table_size, buffer.data() + pos, ret.size());
        memcpy(buffer.data(), p->e.table().data(), table_size);
        return {buffer, std::move(buffer)};
    }
    else {
        memcpy((byte_t*)ret.data() - table_size, p->e.table().data(), table_size);
        return {
            fstring(ret.data() - table_size, ret.size() + table_size),
            std::move(ret_bytes.buffer)
        };
    }
}

size_t decode_o1(fstring data, valvec<byte_t>* record, TerarkContext* context) {
    size_t table_size;
    std::unique_ptr<decoder_o1> d(new decoder_o1(data, &table_size));
    size_t read = d->decode(data.substr(table_size), record, context);
    return read == 0 ? 0 : table_size + read;
}

EntropyBytes encode_o2(fstring record, TerarkContext* context) {
    struct encoder_mem {
        freq_hist_o2 hist;
        encoder_o2 e;
    };
    std::unique_ptr<encoder_mem> p(new encoder_mem);
    p->hist.add_record(record);
    p->hist.finish();
    p->hist.normalise(TOTFREQ);
    p->e.init(p->hist.histogram());
    auto ret_bytes = p->e.encode(record, context);
    auto ret = ret_bytes.data;
    auto buffer = context->alloc();
    assert(ret.udata() + ret.size() == buffer.data() + buffer.size());
    size_t table_size = p->e.table().size();
    if (buffer.data() + table_size > ret.udata()) {
        size_t pos = ret.udata() - buffer.data();
        buffer.resize_no_init(table_size + ret.size());
        memmove(buffer.data() + table_size, buffer.data() + pos, ret.size());
        memcpy(buffer.data(), p->e.table().data(), table_size);
        return {buffer, std::move(buffer)};
    }
    else {
        memcpy((byte_t*)ret.data() - table_size, p->e.table().data(), table_size);
        return {
            fstring(ret.data() - table_size, ret.size() + table_size),
            std::move(ret_bytes.buffer)
        };
    }
}

size_t decode_o2(fstring data, valvec<byte_t>* record, TerarkContext* context) {
    size_t table_size;
    std::unique_ptr<decoder_o2> d(new decoder_o2(data, &table_size));
    size_t read = d->decode(data.substr(table_size), record, context);
    return read == 0 ? 0 : table_size + read;
}

// --------------------------------------------------------------------------

encoder::encoder() {
}

encoder::encoder(const freq_hist::histogram_t& hist) {
    init(hist);
}

void encoder::init(const freq_hist::histogram_t& hist) {
    table_.resize(257 * 3);
    byte_t *cp = table_.data();
    Rans64BuildCTable(&cp, hist.o0, syms_);
    table_.resize(cp - table_.data());
}

const valvec<byte_t>& encoder::table() const {
    return table_;
}

EntropyBytes encoder::encode(fstring record, TerarkContext* context) const {
    if (record.size() < pow_t<RECORD_MAX_SIZE, 1>::value) {
        return encode_xN<1>(record, context, true);
    }
    else if (record.size() < pow_t<RECORD_MAX_SIZE, 2>::value) {
        return encode_xN<2>(record, context, true);
    }
    else if (record.size() < pow_t<RECORD_MAX_SIZE, 4>::value) {
        return encode_xN<4>(record, context, true);
    }
    else {
        return encode_xN<8>(record, context, true);
    }
}

EntropyBytes encoder::encode_x1(fstring record, TerarkContext* context) const {
    return encode_xN<1>(record, context, false);
}

EntropyBytes encoder::encode_x2(fstring record, TerarkContext* context) const {
    return encode_xN<2>(record, context, false);
}

EntropyBytes encoder::encode_x4(fstring record, TerarkContext* context) const {
    return encode_xN<4>(record, context, false);
}

EntropyBytes encoder::encode_x8(fstring record, TerarkContext* context) const {
    return encode_xN<8>(record, context, false);
}

template<size_t N>
EntropyBytes encoder::encode_xN(fstring record, TerarkContext* context, bool check) const {
    if (record.size() >= pow_t<RECORD_MAX_SIZE, N>::value) {
        return {{ nullptr, ptrdiff_t(0) }, {}};
    }
    auto ctx_buffer = context->alloc();
    ctx_buffer.resize(record.size() * 5 / 4 + 8 * N + 8);

#define w7 (N == 8 ? 7 : 0)
#define w6 (N == 8 ? 6 : 0)
#define w5 (N == 8 ? 5 : 0)
#define w4 (N == 8 ? 4 : 0)
#define w3 (N >= 4 ? 3 : 0)
#define w2 (N >= 4 ? 2 : 0)
#define w1 (N >= 2 ? 1 : 0)
#define w0 0

    Rans64State rans[N];
    size_t size = record.size();
    const byte_t* end = record.udata() + size;

    if (w7 == 7) Rans64EncInit(&rans[w7], &end, &size);
    if (w6 == 6) Rans64EncInit(&rans[w6], &end, &size);
    if (w5 == 5) Rans64EncInit(&rans[w5], &end, &size);
    if (w4 == 4) Rans64EncInit(&rans[w4], &end, &size);
    if (w3 == 3) Rans64EncInit(&rans[w3], &end, &size);
    if (w2 == 2) Rans64EncInit(&rans[w2], &end, &size);
    if (w1 == 1) Rans64EncInit(&rans[w1], &end, &size);
    if (w0 == 0) Rans64EncInit(&rans[w0], &end, &size);

    byte_t* ptr = ctx_buffer.data() + ctx_buffer.size();
    const byte_t* record_data = record.udata();

    for (intptr_t i = (intptr_t)size - 1, e = (intptr_t)(size - size % N) - 1; N > 1 && i > e; --i) {
        const Rans64EncSymbol* s = &syms_[record_data[i]];
        Rans64EncRenorm(&rans[N - 1], &ptr, ctx_buffer, s->freq, TF_SHIFT);
        Rans64EncPutSymbol(&rans[N - 1], s, TF_SHIFT);
    }

    if (size >= N) {
        intptr_t i[N];

        if (w7 == 7) i[w7] = (intptr_t)size / N * 8 - 1;
        if (w6 == 6) i[w6] = (intptr_t)size / N * 7 - 1;
        if (w5 == 5) i[w5] = (intptr_t)size / N * 6 - 1;
        if (w4 == 4) i[w4] = (intptr_t)size / N * 5 - 1;
        if (w3 == 3) i[w3] = (intptr_t)size / N * 4 - 1;
        if (w2 == 2) i[w2] = (intptr_t)size / N * 3 - 1;
        if (w1 == 1) i[w1] = (intptr_t)size / N * 2 - 1;
        if (w0 == 0) i[w0] = (intptr_t)size / N * 1 - 1;

        while (i[w0] >= 0) {
            byte_t c[N]; const Rans64EncSymbol* s[N];

            if (w7 == 7) s[w7] = &syms_[c[w7] = record_data[i[w7]]];
            if (w6 == 6) s[w6] = &syms_[c[w6] = record_data[i[w6]]];
            if (w5 == 5) s[w5] = &syms_[c[w5] = record_data[i[w5]]];
            if (w4 == 4) s[w4] = &syms_[c[w4] = record_data[i[w4]]];
            if (w3 == 3) s[w3] = &syms_[c[w3] = record_data[i[w3]]];
            if (w2 == 2) s[w2] = &syms_[c[w2] = record_data[i[w2]]];
            if (w1 == 1) s[w1] = &syms_[c[w1] = record_data[i[w1]]];
            if (w0 == 0) s[w0] = &syms_[c[w0] = record_data[i[w0]]];

            if (w7 == 7) Rans64EncRenorm(&rans[w7], &ptr, ctx_buffer, s[w7]->freq, TF_SHIFT);
            if (w6 == 6) Rans64EncRenorm(&rans[w6], &ptr, ctx_buffer, s[w6]->freq, TF_SHIFT);
            if (w5 == 5) Rans64EncRenorm(&rans[w5], &ptr, ctx_buffer, s[w5]->freq, TF_SHIFT);
            if (w4 == 4) Rans64EncRenorm(&rans[w4], &ptr, ctx_buffer, s[w4]->freq, TF_SHIFT);
            if (w3 == 3) Rans64EncRenorm(&rans[w3], &ptr, ctx_buffer, s[w3]->freq, TF_SHIFT);
            if (w2 == 2) Rans64EncRenorm(&rans[w2], &ptr, ctx_buffer, s[w2]->freq, TF_SHIFT);
            if (w1 == 1) Rans64EncRenorm(&rans[w1], &ptr, ctx_buffer, s[w1]->freq, TF_SHIFT);
            if (w0 == 0) Rans64EncRenorm(&rans[w0], &ptr, ctx_buffer, s[w0]->freq, TF_SHIFT);

            if (w7 == 7) Rans64EncPutSymbol(&rans[w7], s[w7], TF_SHIFT);
            if (w6 == 6) Rans64EncPutSymbol(&rans[w6], s[w6], TF_SHIFT);
            if (w5 == 5) Rans64EncPutSymbol(&rans[w5], s[w5], TF_SHIFT);
            if (w4 == 4) Rans64EncPutSymbol(&rans[w4], s[w4], TF_SHIFT);
            if (w3 == 3) Rans64EncPutSymbol(&rans[w3], s[w3], TF_SHIFT);
            if (w2 == 2) Rans64EncPutSymbol(&rans[w2], s[w2], TF_SHIFT);
            if (w1 == 1) Rans64EncPutSymbol(&rans[w1], s[w1], TF_SHIFT);
            if (w0 == 0) Rans64EncPutSymbol(&rans[w0], s[w0], TF_SHIFT);

            if (w7 == 7) --i[w7];
            if (w6 == 6) --i[w6];
            if (w5 == 5) --i[w5];
            if (w4 == 4) --i[w4];
            if (w3 == 3) --i[w3];
            if (w2 == 2) --i[w2];
            if (w1 == 1) --i[w1];
            if (w0 == 0) --i[w0];
        }
    }

    size = record.size();

    if (w7 == 7) Rans64EncFlush(&rans[w7], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w6 == 6) Rans64EncFlush(&rans[w6], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w5 == 5) Rans64EncFlush(&rans[w5], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w4 == 4) Rans64EncFlush(&rans[w4], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w3 == 3) Rans64EncFlush(&rans[w3], &ptr, ctx_buffer, &size, XCheck{ check, N > 4 });  // 1:0 2:0 4:0 8:1
    if (w2 == 2) Rans64EncFlush(&rans[w2], &ptr, ctx_buffer, &size, XCheck{ check, N > 4 });  // 1:0 2:0 4:0 8:1
    if (w1 == 1) Rans64EncFlush(&rans[w1], &ptr, ctx_buffer, &size, XCheck{ check, N > 2 });  // 1:0 2:0 4:1 8:1
    if (w0 == 0) Rans64EncFlush(&rans[w0], &ptr, ctx_buffer, &size, XCheck{ check, N > 1 });  // 1:0 2:1 4:1 8:1

    assert(size == 0);

#undef w7
#undef w6
#undef w5
#undef w4
#undef w3
#undef w2
#undef w1
#undef w0

    return EntropyBytes{
        fstring{ptr, ctx_buffer.data() + ctx_buffer.size() - ptr},
        std::move(ctx_buffer)
    };
}

// --------------------------------------------------------------------------

decoder::decoder() {
}

decoder::decoder(fstring table, size_t* psize) {
    init(table, psize);
}

void decoder::init(fstring table, size_t* psize) {
    const byte_t *cp = table.udata();

    Rans64BuildDTable(&cp, ari_, syms_);
    if (psize != nullptr) {
        *psize = cp - table.udata();
    }
}

size_t decoder::decode(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    size_t read;
    if ((read = decode_xN<8>(data, record, context, true)) > 0) {
        return read;
    }
    if ((read = decode_xN<4>(data, record, context, true)) > 0) {
        return read;
    }
    if ((read = decode_xN<2>(data, record, context, true)) > 0) {
        return read;
    }
    if ((read = decode_xN<1>(data, record, context, true)) > 0) {
        return read;
    }
    return 0;
}

size_t decoder::decode_x1(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<1>(data, record, context, false);
}

size_t decoder::decode_x2(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<2>(data, record, context, false);
}

size_t decoder::decode_x4(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<4>(data, record, context, false);
}

size_t decoder::decode_x8(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<8>(data, record, context, false);
}

template<size_t N>
size_t decoder::decode_xN(fstring data, valvec<byte_t>* record, TerarkContext* context, bool check) const {
    record->risk_set_size(0);
    if (data.size() < N * 2) {
        return 0;
    }

#define w0 0
#define w1 (N >= 2 ? 1 : 0)
#define w2 (N >= 4 ? 2 : 0)
#define w3 (N >= 4 ? 3 : 0)
#define w4 (N == 8 ? 4 : 0)
#define w5 (N == 8 ? 5 : 0)
#define w6 (N == 8 ? 6 : 0)
#define w7 (N == 8 ? 7 : 0)

    Rans64State rans[N];
    const byte_t* ptr = data.udata();
    const byte_t* end = data.udata() + data.size();
    size_t record_size = 0;

    if (w0 == 0) if (!Rans64DecInit(&rans[w0], &record_size, &ptr, end, XCheck{ check, N > 1 })) return 0;
    if (w1 == 1) if (!Rans64DecInit(&rans[w1], &record_size, &ptr, end, XCheck{ check, N > 2 })) return 0;
    if (w2 == 2) if (!Rans64DecInit(&rans[w2], &record_size, &ptr, end, XCheck{ check, N > 4 })) return 0;
    if (w3 == 3) if (!Rans64DecInit(&rans[w3], &record_size, &ptr, end, XCheck{ check, N > 4 })) return 0;
    if (w4 == 4) if (!Rans64DecInit(&rans[w4], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;
    if (w5 == 5) if (!Rans64DecInit(&rans[w5], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;
    if (w6 == 6) if (!Rans64DecInit(&rans[w6], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;
    if (w7 == 7) if (!Rans64DecInit(&rans[w7], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;

    record->resize_no_init(record_size);
    byte_t* o[N];
    size_t size = record_size > N * (RANS_L_BITS / 8) ? record_size - N * (RANS_L_BITS / 8) : 0;

    if (w0 == 0) o[w0] = record->data() + size / N * 0;
    if (w1 == 1) o[w1] = record->data() + size / N * 1;
    if (w2 == 2) o[w2] = record->data() + size / N * 2;
    if (w3 == 3) o[w3] = record->data() + size / N * 3;
    if (w4 == 4) o[w4] = record->data() + size / N * 4;
    if (w5 == 5) o[w5] = record->data() + size / N * 5;
    if (w6 == 6) o[w6] = record->data() + size / N * 6;
    if (w7 == 7) o[w7] = record->data() + size / N * 7;

    for (size_t i = 0, e = size / N; i < e; ++i) {
        uint32_t m[N];

        if (w0 == 0) m[w0] = rans[w0] & (TOTFREQ - 1);
        if (w1 == 1) m[w1] = rans[w1] & (TOTFREQ - 1);
        if (w2 == 2) m[w2] = rans[w2] & (TOTFREQ - 1);
        if (w3 == 3) m[w3] = rans[w3] & (TOTFREQ - 1);
        if (w4 == 4) m[w4] = rans[w4] & (TOTFREQ - 1);
        if (w5 == 5) m[w5] = rans[w5] & (TOTFREQ - 1);
        if (w6 == 6) m[w6] = rans[w6] & (TOTFREQ - 1);
        if (w7 == 7) m[w7] = rans[w7] & (TOTFREQ - 1);

        byte_t c[N];

        if (w0 == 0) c[w0] = ari_[m[w0]];
        if (w1 == 1) c[w1] = ari_[m[w1]];
        if (w2 == 2) c[w2] = ari_[m[w2]];
        if (w3 == 3) c[w3] = ari_[m[w3]];
        if (w4 == 4) c[w4] = ari_[m[w4]];
        if (w5 == 5) c[w5] = ari_[m[w5]];
        if (w6 == 6) c[w6] = ari_[m[w6]];
        if (w7 == 7) c[w7] = ari_[m[w7]];

        if (w0 == 0) rans[w0] = syms_[c[w0]].freq * (rans[w0] >> TF_SHIFT);
        if (w1 == 1) rans[w1] = syms_[c[w1]].freq * (rans[w1] >> TF_SHIFT);
        if (w2 == 2) rans[w2] = syms_[c[w2]].freq * (rans[w2] >> TF_SHIFT);
        if (w3 == 3) rans[w3] = syms_[c[w3]].freq * (rans[w3] >> TF_SHIFT);
        if (w4 == 4) rans[w4] = syms_[c[w4]].freq * (rans[w4] >> TF_SHIFT);
        if (w5 == 5) rans[w5] = syms_[c[w5]].freq * (rans[w5] >> TF_SHIFT);
        if (w6 == 6) rans[w6] = syms_[c[w6]].freq * (rans[w6] >> TF_SHIFT);
        if (w7 == 7) rans[w7] = syms_[c[w7]].freq * (rans[w7] >> TF_SHIFT);

        if (w0 == 0) rans[w0] += m[w0] - syms_[c[w0]].start;
        if (w1 == 1) rans[w1] += m[w1] - syms_[c[w1]].start;
        if (w2 == 2) rans[w2] += m[w2] - syms_[c[w2]].start;
        if (w3 == 3) rans[w3] += m[w3] - syms_[c[w3]].start;
        if (w4 == 4) rans[w4] += m[w4] - syms_[c[w4]].start;
        if (w5 == 5) rans[w5] += m[w5] - syms_[c[w5]].start;
        if (w6 == 6) rans[w6] += m[w6] - syms_[c[w6]].start;
        if (w7 == 7) rans[w7] += m[w7] - syms_[c[w7]].start;

        if (w0 == 0) *o[w0]++ = c[w0];
        if (w1 == 1) *o[w1]++ = c[w1];
        if (w2 == 2) *o[w2]++ = c[w2];
        if (w3 == 3) *o[w3]++ = c[w3];
        if (w4 == 4) *o[w4]++ = c[w4];
        if (w5 == 5) *o[w5]++ = c[w5];
        if (w6 == 6) *o[w6]++ = c[w6];
        if (w7 == 7) *o[w7]++ = c[w7];

        if (w0 == 0) if (!Rans64DecRenorm(&rans[w0], &ptr, end)) return 0;
        if (w1 == 1) if (!Rans64DecRenorm(&rans[w1], &ptr, end)) return 0;
        if (w2 == 2) if (!Rans64DecRenorm(&rans[w2], &ptr, end)) return 0;
        if (w3 == 3) if (!Rans64DecRenorm(&rans[w3], &ptr, end)) return 0;
        if (w4 == 4) if (!Rans64DecRenorm(&rans[w4], &ptr, end)) return 0;
        if (w5 == 5) if (!Rans64DecRenorm(&rans[w5], &ptr, end)) return 0;
        if (w6 == 6) if (!Rans64DecRenorm(&rans[w6], &ptr, end)) return 0;
        if (w7 == 7) if (!Rans64DecRenorm(&rans[w7], &ptr, end)) return 0;
    }
    for (size_t i = 0, e = size % N; N > 1 && i < e; ++i) {
        uint32_t m = rans[N - 1] & (TOTFREQ - 1);
        byte_t c = ari_[m];
        rans[N - 1] = syms_[c].freq * (rans[N - 1] >> TF_SHIFT);
        rans[N - 1] += m - syms_[c].start;
        *o[N - 1]++ = c;
        if (!Rans64DecRenorm(&rans[N - 1], &ptr, end)) return 0;
    }

    byte_t* optr = record->data() + size;
    byte_t* oend = record->data() + record_size;

    if (w0 == 0) if (!Rans64DecTail(&rans[w0], &optr, oend)) return 0;
    if (w1 == 1) if (!Rans64DecTail(&rans[w1], &optr, oend)) return 0;
    if (w2 == 2) if (!Rans64DecTail(&rans[w2], &optr, oend)) return 0;
    if (w3 == 3) if (!Rans64DecTail(&rans[w3], &optr, oend)) return 0;
    if (w4 == 4) if (!Rans64DecTail(&rans[w4], &optr, oend)) return 0;
    if (w5 == 5) if (!Rans64DecTail(&rans[w5], &optr, oend)) return 0;
    if (w6 == 6) if (!Rans64DecTail(&rans[w6], &optr, oend)) return 0;
    if (w7 == 7) if (!Rans64DecTail(&rans[w7], &optr, oend)) return 0;

#undef w0
#undef w1
#undef w2
#undef w3
#undef w4
#undef w5
#undef w6
#undef w7

    return ptr - data.udata();
}

// --------------------------------------------------------------------------

encoder_o1::encoder_o1() {
}

encoder_o1::encoder_o1(const freq_hist_o1::histogram_t& hist) {
    init(hist);
}

void encoder_o1::init(const freq_hist_o1::histogram_t& hist) {
    table_.resize(258 * 257 * 3);
    byte_t *cp = table_.data();
    size_t rle_i, i;
    *cp++ = 1;  // version

    if (std::find_if(hist.o1_size, hist.o1_size + 256, [](uint64_t v) { return v > 0; }) == hist.o1_size + 256) {
        *cp++ = 0;  // ingore;
    }
    else {
        *cp++ = 1;  // begin
        for (rle_i = i = 0; i < 256; i++) {
            if (hist.o1_size[i] == 0)
                continue;
            // Store frequency table
            if (rle_i) {
                rle_i--;
            }
            else {
                *cp++ = i;
                // FIXME: could use order-0 statistics to observe which alphabet
                // symbols are present and base RLE on that ordering instead.
                if (i && hist.o1_size[i - 1]) {
                    for (rle_i = i + 1; rle_i < 256 && hist.o1_size[rle_i]; rle_i++)
                        ;
                    rle_i -= i + 1;
                    *cp++ = rle_i;
                }
            }
            Rans64BuildCTable(&cp, hist.o1[i], syms_[i]);
        }
        *cp++ = 0;  // end
    }
    Rans64BuildCTable(&cp, hist.o0, syms_[256]);
    table_.risk_set_size(cp - table_.data());
    assert(table_.size() < 258 * 257 * 3);
    TerarkContext context;
    auto table_ans_bytes = rANS_static_64::encode(fstring(table_).substr(1), &context);
    auto table_ans = table_ans_bytes.data;
    if (table_ans.size() < table_.size()) {
        table_[0] = 255;  // encoded
        memcpy(table_.data() + 1, table_ans.data(), table_ans.size());
        table_.risk_set_size(table_ans.size() + 1);
    }
    table_.shrink_to_fit();
}

const valvec<byte_t>& encoder_o1::table() const {
    return table_;
}

EntropyBytes encoder_o1::encode(fstring record, TerarkContext* context) const {
    if (record.size() < pow_t<RECORD_MAX_SIZE, 1>::value) {
        return encode_xN<1>(record, context, true);
    }
    else if (record.size() < pow_t<RECORD_MAX_SIZE, 2>::value) {
        return encode_xN<2>(record, context, true);
    }
    else if (record.size() < pow_t<RECORD_MAX_SIZE, 4>::value) {
        return encode_xN<4>(record, context, true);
    }
    else {
        return encode_xN<8>(record, context, true);
    }
}

EntropyBytes encoder_o1::encode_x1(fstring record, TerarkContext* context) const {
    return encode_xN<1>(record, context, false);
}

EntropyBytes encoder_o1::encode_x2(fstring record, TerarkContext* context) const {
    return encode_xN<2>(record, context, false);
}

EntropyBytes encoder_o1::encode_x4(fstring record, TerarkContext* context) const {
    return encode_xN<4>(record, context, false);
}

EntropyBytes encoder_o1::encode_x8(fstring record, TerarkContext* context) const {
    return encode_xN<8>(record, context, false);
}

template<size_t N>
EntropyBytes encoder_o1::encode_xN(fstring record, TerarkContext* context, bool check) const {
    if (record.size() >= pow_t<RECORD_MAX_SIZE, N>::value) {
        return {{ nullptr, ptrdiff_t(0) }, {}};
    }
    auto ctx_buffer = context->alloc();
    ctx_buffer.resize(record.size() * 5 / 4 + 8 * N + 8);

#define w7 (N == 8 ? 7 : 0)
#define w6 (N == 8 ? 6 : 0)
#define w5 (N == 8 ? 5 : 0)
#define w4 (N == 8 ? 4 : 0)
#define w3 (N >= 4 ? 3 : 0)
#define w2 (N >= 4 ? 2 : 0)
#define w1 (N >= 2 ? 1 : 0)
#define w0 0

    Rans64State rans[N];
    size_t size = record.size();
    const byte_t* end = record.udata() + size;

    if (w7 == 7) Rans64EncInit(&rans[w7], &end, &size);
    if (w6 == 6) Rans64EncInit(&rans[w6], &end, &size);
    if (w5 == 5) Rans64EncInit(&rans[w5], &end, &size);
    if (w4 == 4) Rans64EncInit(&rans[w4], &end, &size);
    if (w3 == 3) Rans64EncInit(&rans[w3], &end, &size);
    if (w2 == 2) Rans64EncInit(&rans[w2], &end, &size);
    if (w1 == 1) Rans64EncInit(&rans[w1], &end, &size);
    if (w0 == 0) Rans64EncInit(&rans[w0], &end, &size);

    byte_t* ptr = ctx_buffer.data() + ctx_buffer.size();
    const byte_t* record_data = record.udata();

    for (intptr_t i = (intptr_t)size - 2, e = (intptr_t)(size - size % N) - 1; N > 1 && i >= e; --i) { 
        const Rans64EncSymbol* s = &syms_[i == -1 ? 256 : record_data[i]][record_data[i + 1]];
        Rans64EncRenorm(&rans[N - 1], &ptr, ctx_buffer, s->freq, TF_SHIFT);
        Rans64EncPutSymbol(&rans[N - 1], s, TF_SHIFT);
    }

    if (size >= N) {
        intptr_t i[N];
        byte_t l[N];

        if (w7 == 7) i[w7] = (intptr_t)size / N * 8 - 2;
        if (w6 == 6) i[w6] = (intptr_t)size / N * 7 - 2;
        if (w5 == 5) i[w5] = (intptr_t)size / N * 6 - 2;
        if (w4 == 4) i[w4] = (intptr_t)size / N * 5 - 2;
        if (w3 == 3) i[w3] = (intptr_t)size / N * 4 - 2;
        if (w2 == 2) i[w2] = (intptr_t)size / N * 3 - 2;
        if (w1 == 1) i[w1] = (intptr_t)size / N * 2 - 2;
        if (w0 == 0) i[w0] = (intptr_t)size / N * 1 - 2;

        if (w7 == 7) l[w7] = record_data[i[w7] + 1];
        if (w6 == 6) l[w6] = record_data[i[w6] + 1];
        if (w5 == 5) l[w5] = record_data[i[w5] + 1];
        if (w4 == 4) l[w4] = record_data[i[w4] + 1];
        if (w3 == 3) l[w3] = record_data[i[w3] + 1];
        if (w2 == 2) l[w2] = record_data[i[w2] + 1];
        if (w1 == 1) l[w1] = record_data[i[w1] + 1];
        if (w0 == 0) l[w0] = record_data[i[w0] + 1];

        while (i[w0] >= 0) {
            byte_t c[N]; const Rans64EncSymbol* s[N];

            if (w7 == 7) s[w7] = &syms_[c[w7] = record_data[i[w7]]][l[w7]];
            if (w6 == 6) s[w6] = &syms_[c[w6] = record_data[i[w6]]][l[w6]];
            if (w5 == 5) s[w5] = &syms_[c[w5] = record_data[i[w5]]][l[w5]];
            if (w4 == 4) s[w4] = &syms_[c[w4] = record_data[i[w4]]][l[w4]];
            if (w3 == 3) s[w3] = &syms_[c[w3] = record_data[i[w3]]][l[w3]];
            if (w2 == 2) s[w2] = &syms_[c[w2] = record_data[i[w2]]][l[w2]];
            if (w1 == 1) s[w1] = &syms_[c[w1] = record_data[i[w1]]][l[w1]];
            if (w0 == 0) s[w0] = &syms_[c[w0] = record_data[i[w0]]][l[w0]];

            if (w7 == 7) Rans64EncRenorm(&rans[w7], &ptr, ctx_buffer, s[w7]->freq, TF_SHIFT);
            if (w6 == 6) Rans64EncRenorm(&rans[w6], &ptr, ctx_buffer, s[w6]->freq, TF_SHIFT);
            if (w5 == 5) Rans64EncRenorm(&rans[w5], &ptr, ctx_buffer, s[w5]->freq, TF_SHIFT);
            if (w4 == 4) Rans64EncRenorm(&rans[w4], &ptr, ctx_buffer, s[w4]->freq, TF_SHIFT);
            if (w3 == 3) Rans64EncRenorm(&rans[w3], &ptr, ctx_buffer, s[w3]->freq, TF_SHIFT);
            if (w2 == 2) Rans64EncRenorm(&rans[w2], &ptr, ctx_buffer, s[w2]->freq, TF_SHIFT);
            if (w1 == 1) Rans64EncRenorm(&rans[w1], &ptr, ctx_buffer, s[w1]->freq, TF_SHIFT);
            if (w0 == 0) Rans64EncRenorm(&rans[w0], &ptr, ctx_buffer, s[w0]->freq, TF_SHIFT);

            if (w7 == 7) Rans64EncPutSymbol(&rans[w7], s[w7], TF_SHIFT);
            if (w6 == 6) Rans64EncPutSymbol(&rans[w6], s[w6], TF_SHIFT);
            if (w5 == 5) Rans64EncPutSymbol(&rans[w5], s[w5], TF_SHIFT);
            if (w4 == 4) Rans64EncPutSymbol(&rans[w4], s[w4], TF_SHIFT);
            if (w3 == 3) Rans64EncPutSymbol(&rans[w3], s[w3], TF_SHIFT);
            if (w2 == 2) Rans64EncPutSymbol(&rans[w2], s[w2], TF_SHIFT);
            if (w1 == 1) Rans64EncPutSymbol(&rans[w1], s[w1], TF_SHIFT);
            if (w0 == 0) Rans64EncPutSymbol(&rans[w0], s[w0], TF_SHIFT);

            if (w7 == 7) l[w7] = c[w7];
            if (w6 == 6) l[w6] = c[w6];
            if (w5 == 5) l[w5] = c[w5];
            if (w4 == 4) l[w4] = c[w4];
            if (w3 == 3) l[w3] = c[w3];
            if (w2 == 2) l[w2] = c[w2];
            if (w1 == 1) l[w1] = c[w1];
            if (w0 == 0) l[w0] = c[w0];

            if (w7 == 7) --i[w7];
            if (w6 == 6) --i[w6];
            if (w5 == 5) --i[w5];
            if (w4 == 4) --i[w4];
            if (w3 == 3) --i[w3];
            if (w2 == 2) --i[w2];
            if (w1 == 1) --i[w1];
            if (w0 == 0) --i[w0];
        }

        if (w7 == 7) Rans64EncRenorm(&rans[w7], &ptr, ctx_buffer, syms_[256][l[w7]].freq, TF_SHIFT);
        if (w6 == 6) Rans64EncRenorm(&rans[w6], &ptr, ctx_buffer, syms_[256][l[w6]].freq, TF_SHIFT);
        if (w5 == 5) Rans64EncRenorm(&rans[w5], &ptr, ctx_buffer, syms_[256][l[w5]].freq, TF_SHIFT);
        if (w4 == 4) Rans64EncRenorm(&rans[w4], &ptr, ctx_buffer, syms_[256][l[w4]].freq, TF_SHIFT);
        if (w3 == 3) Rans64EncRenorm(&rans[w3], &ptr, ctx_buffer, syms_[256][l[w3]].freq, TF_SHIFT);
        if (w2 == 2) Rans64EncRenorm(&rans[w2], &ptr, ctx_buffer, syms_[256][l[w2]].freq, TF_SHIFT);
        if (w1 == 1) Rans64EncRenorm(&rans[w1], &ptr, ctx_buffer, syms_[256][l[w1]].freq, TF_SHIFT);
        if (w0 == 0) Rans64EncRenorm(&rans[w0], &ptr, ctx_buffer, syms_[256][l[w0]].freq, TF_SHIFT);

        if (w7 == 7) Rans64EncPutSymbol(&rans[w7], &syms_[256][l[w7]], TF_SHIFT);
        if (w6 == 6) Rans64EncPutSymbol(&rans[w6], &syms_[256][l[w6]], TF_SHIFT);
        if (w5 == 5) Rans64EncPutSymbol(&rans[w5], &syms_[256][l[w5]], TF_SHIFT);
        if (w4 == 4) Rans64EncPutSymbol(&rans[w4], &syms_[256][l[w4]], TF_SHIFT);
        if (w3 == 3) Rans64EncPutSymbol(&rans[w3], &syms_[256][l[w3]], TF_SHIFT);
        if (w2 == 2) Rans64EncPutSymbol(&rans[w2], &syms_[256][l[w2]], TF_SHIFT);
        if (w1 == 1) Rans64EncPutSymbol(&rans[w1], &syms_[256][l[w1]], TF_SHIFT);
        if (w0 == 0) Rans64EncPutSymbol(&rans[w0], &syms_[256][l[w0]], TF_SHIFT);

    }

    size = record.size();

    if (w7 == 7) Rans64EncFlush(&rans[w7], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w6 == 6) Rans64EncFlush(&rans[w6], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w5 == 5) Rans64EncFlush(&rans[w5], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w4 == 4) Rans64EncFlush(&rans[w4], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w3 == 3) Rans64EncFlush(&rans[w3], &ptr, ctx_buffer, &size, XCheck{ check, N > 4 });  // 1:0 2:0 4:0 8:1
    if (w2 == 2) Rans64EncFlush(&rans[w2], &ptr, ctx_buffer, &size, XCheck{ check, N > 4 });  // 1:0 2:0 4:0 8:1
    if (w1 == 1) Rans64EncFlush(&rans[w1], &ptr, ctx_buffer, &size, XCheck{ check, N > 2 });  // 1:0 2:0 4:1 8:1
    if (w0 == 0) Rans64EncFlush(&rans[w0], &ptr, ctx_buffer, &size, XCheck{ check, N > 1 });  // 1:0 2:1 4:1 8:1

    assert(size == 0);

#undef w7
#undef w6
#undef w5
#undef w4
#undef w3
#undef w2
#undef w1
#undef w0

    return EntropyBytes{
        fstring{ptr, ctx_buffer.data() + ctx_buffer.size() - ptr},
        std::move(ctx_buffer)
    };
}

// --------------------------------------------------------------------------

decoder_o1::decoder_o1() {
}

decoder_o1::decoder_o1(fstring table, size_t* psize) {
    init(table, psize);
}

void decoder_o1::init(fstring table, size_t* psize) {
    size_t i, rle_i;
    const byte_t *cp = table.udata();
    const byte_t *end = cp + table.size();

    memset(syms_, 0, sizeof syms_);
    memset(ari_, 0, sizeof ari_);

    valvec<byte_t> table_ans;
    if (*cp++ == 255) {
        TerarkContext context;
        size_t read = rANS_static_64::decode(table.substr(1), &table_ans, &context);
        if (psize != nullptr) {
            *psize = read + 1;
        }
        cp = table_ans.data();
        end = cp + table_ans.size();
    }

    if (*cp++ != 0) {
        rle_i = 0;
        i = *cp++;
        do {
            Rans64BuildDTable(&cp, ari_[i], syms_[i]);
            if (!rle_i && i + 1 == *cp) {
                i = *cp++;
                rle_i = *cp++;
            }
            else if (rle_i) {
                rle_i--;
                i++;
            }
            else {
                i = *cp++;
            }
        } while (i);
    }
    Rans64BuildDTable(&cp, ari_[256], syms_[256]);
    if (psize != nullptr && end == table.udata() + table.size()) {
        *psize = cp - table.udata();
    }
}

size_t decoder_o1::decode(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    size_t read;
    if ((read = decode_xN<8>(data, record, context, true)) > 0) {
        return read;
    }
    if ((read = decode_xN<4>(data, record, context, true)) > 0) {
        return read;
    }
    if ((read = decode_xN<2>(data, record, context, true)) > 0) {
        return read;
    }
    if ((read = decode_xN<1>(data, record, context, true)) > 0) {
        return read;
    }
    return 0;
}

size_t decoder_o1::decode_x1(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<1>(data, record, context, false);
}

size_t decoder_o1::decode_x2(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<2>(data, record, context, false);
}

size_t decoder_o1::decode_x4(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<4>(data, record, context, false);
}

size_t decoder_o1::decode_x8(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<8>(data, record, context, false);
}

template<size_t N>
size_t decoder_o1::decode_xN(fstring data, valvec<byte_t>* record, TerarkContext* context, bool check) const {
    record->risk_set_size(0);
    if(data.size() < N * 2) {
        return 0;
    }

#define w0 0
#define w1 (N >= 2 ? 1 : 0)
#define w2 (N >= 4 ? 2 : 0)
#define w3 (N >= 4 ? 3 : 0)
#define w4 (N == 8 ? 4 : 0)
#define w5 (N == 8 ? 5 : 0)
#define w6 (N == 8 ? 6 : 0)
#define w7 (N == 8 ? 7 : 0)

    Rans64State rans[N];
    const byte_t* ptr = data.udata();
    const byte_t* end = data.udata() + data.size();
    size_t record_size = 0;

    if (w0 == 0) if (!Rans64DecInit(&rans[w0], &record_size, &ptr, end, XCheck{ check, N > 1 })) return 0;
    if (w1 == 1) if (!Rans64DecInit(&rans[w1], &record_size, &ptr, end, XCheck{ check, N > 2 })) return 0;
    if (w2 == 2) if (!Rans64DecInit(&rans[w2], &record_size, &ptr, end, XCheck{ check, N > 4 })) return 0;
    if (w3 == 3) if (!Rans64DecInit(&rans[w3], &record_size, &ptr, end, XCheck{ check, N > 4 })) return 0;
    if (w4 == 4) if (!Rans64DecInit(&rans[w4], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;
    if (w5 == 5) if (!Rans64DecInit(&rans[w5], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;
    if (w6 == 6) if (!Rans64DecInit(&rans[w6], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;
    if (w7 == 7) if (!Rans64DecInit(&rans[w7], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;

    record->resize_no_init(record_size);
    size_t l[N];
    byte_t* o[N];
    size_t size = record_size > N * (RANS_L_BITS / 8) ? record_size - N * (RANS_L_BITS / 8) : 0;

    if (w0 == 0) l[w0] = 256;
    if (w1 == 1) l[w1] = 256;
    if (w2 == 2) l[w2] = 256;
    if (w3 == 3) l[w3] = 256;
    if (w4 == 4) l[w4] = 256;
    if (w5 == 5) l[w5] = 256;
    if (w6 == 6) l[w6] = 256;
    if (w7 == 7) l[w7] = 256;

    if (w0 == 0) o[w0] = record->data() + size / N * 0;
    if (w1 == 1) o[w1] = record->data() + size / N * 1;
    if (w2 == 2) o[w2] = record->data() + size / N * 2;
    if (w3 == 3) o[w3] = record->data() + size / N * 3;
    if (w4 == 4) o[w4] = record->data() + size / N * 4;
    if (w5 == 5) o[w5] = record->data() + size / N * 5;
    if (w6 == 6) o[w6] = record->data() + size / N * 6;
    if (w7 == 7) o[w7] = record->data() + size / N * 7;

    for (size_t i = 0, e = size / N; i < e; ++i) {
        uint32_t m[N];

        if (w0 == 0) m[w0] = rans[w0] & (TOTFREQ - 1);
        if (w1 == 1) m[w1] = rans[w1] & (TOTFREQ - 1);
        if (w2 == 2) m[w2] = rans[w2] & (TOTFREQ - 1);
        if (w3 == 3) m[w3] = rans[w3] & (TOTFREQ - 1);
        if (w4 == 4) m[w4] = rans[w4] & (TOTFREQ - 1);
        if (w5 == 5) m[w5] = rans[w5] & (TOTFREQ - 1);
        if (w6 == 6) m[w6] = rans[w6] & (TOTFREQ - 1);
        if (w7 == 7) m[w7] = rans[w7] & (TOTFREQ - 1);

        byte_t c[N];

        if (w0 == 0) c[w0] = ari_[l[w0]][m[w0]];
        if (w1 == 1) c[w1] = ari_[l[w1]][m[w1]];
        if (w2 == 2) c[w2] = ari_[l[w2]][m[w2]];
        if (w3 == 3) c[w3] = ari_[l[w3]][m[w3]];
        if (w4 == 4) c[w4] = ari_[l[w4]][m[w4]];
        if (w5 == 5) c[w5] = ari_[l[w5]][m[w5]];
        if (w6 == 6) c[w6] = ari_[l[w6]][m[w6]];
        if (w7 == 7) c[w7] = ari_[l[w7]][m[w7]];

        if (w0 == 0) rans[w0] = syms_[l[w0]][c[w0]].freq * (rans[w0] >> TF_SHIFT);
        if (w1 == 1) rans[w1] = syms_[l[w1]][c[w1]].freq * (rans[w1] >> TF_SHIFT);
        if (w2 == 2) rans[w2] = syms_[l[w2]][c[w2]].freq * (rans[w2] >> TF_SHIFT);
        if (w3 == 3) rans[w3] = syms_[l[w3]][c[w3]].freq * (rans[w3] >> TF_SHIFT);
        if (w4 == 4) rans[w4] = syms_[l[w4]][c[w4]].freq * (rans[w4] >> TF_SHIFT);
        if (w5 == 5) rans[w5] = syms_[l[w5]][c[w5]].freq * (rans[w5] >> TF_SHIFT);
        if (w6 == 6) rans[w6] = syms_[l[w6]][c[w6]].freq * (rans[w6] >> TF_SHIFT);
        if (w7 == 7) rans[w7] = syms_[l[w7]][c[w7]].freq * (rans[w7] >> TF_SHIFT);

        if (w0 == 0) rans[w0] += m[w0] - syms_[l[w0]][c[w0]].start;
        if (w1 == 1) rans[w1] += m[w1] - syms_[l[w1]][c[w1]].start;
        if (w2 == 2) rans[w2] += m[w2] - syms_[l[w2]][c[w2]].start;
        if (w3 == 3) rans[w3] += m[w3] - syms_[l[w3]][c[w3]].start;
        if (w4 == 4) rans[w4] += m[w4] - syms_[l[w4]][c[w4]].start;
        if (w5 == 5) rans[w5] += m[w5] - syms_[l[w5]][c[w5]].start;
        if (w6 == 6) rans[w6] += m[w6] - syms_[l[w6]][c[w6]].start;
        if (w7 == 7) rans[w7] += m[w7] - syms_[l[w7]][c[w7]].start;

        if (w0 == 0) *o[w0]++ = l[w0] = c[w0];
        if (w1 == 1) *o[w1]++ = l[w1] = c[w1];
        if (w2 == 2) *o[w2]++ = l[w2] = c[w2];
        if (w3 == 3) *o[w3]++ = l[w3] = c[w3];
        if (w4 == 4) *o[w4]++ = l[w4] = c[w4];
        if (w5 == 5) *o[w5]++ = l[w5] = c[w5];
        if (w6 == 6) *o[w6]++ = l[w6] = c[w6];
        if (w7 == 7) *o[w7]++ = l[w7] = c[w7];

        if (w0 == 0) if (!Rans64DecRenorm(&rans[w0], &ptr, end)) return 0;
        if (w1 == 1) if (!Rans64DecRenorm(&rans[w1], &ptr, end)) return 0;
        if (w2 == 2) if (!Rans64DecRenorm(&rans[w2], &ptr, end)) return 0;
        if (w3 == 3) if (!Rans64DecRenorm(&rans[w3], &ptr, end)) return 0;
        if (w4 == 4) if (!Rans64DecRenorm(&rans[w4], &ptr, end)) return 0;
        if (w5 == 5) if (!Rans64DecRenorm(&rans[w5], &ptr, end)) return 0;
        if (w6 == 6) if (!Rans64DecRenorm(&rans[w6], &ptr, end)) return 0;
        if (w7 == 7) if (!Rans64DecRenorm(&rans[w7], &ptr, end)) return 0;
    }
    for (size_t i = 0, e = size % N; N > 1 && i < e; ++i) {
        uint32_t m = rans[N - 1] & (TOTFREQ - 1);
        byte_t c = ari_[l[N - 1]][m];
        rans[N - 1] = syms_[l[N - 1]][c].freq * (rans[N - 1] >> TF_SHIFT);
        rans[N - 1] += m - syms_[l[N - 1]][c].start;
        *o[N - 1]++ = l[N - 1] = c;
        if (!Rans64DecRenorm(&rans[N - 1], &ptr, end)) return 0;
    }

    byte_t* optr = record->data() + size;
    byte_t* oend = record->data() + record_size;

    if (w0 == 0) if (!Rans64DecTail(&rans[w0], &optr, oend)) return 0;
    if (w1 == 1) if (!Rans64DecTail(&rans[w1], &optr, oend)) return 0;
    if (w2 == 2) if (!Rans64DecTail(&rans[w2], &optr, oend)) return 0;
    if (w3 == 3) if (!Rans64DecTail(&rans[w3], &optr, oend)) return 0;
    if (w4 == 4) if (!Rans64DecTail(&rans[w4], &optr, oend)) return 0;
    if (w5 == 5) if (!Rans64DecTail(&rans[w5], &optr, oend)) return 0;
    if (w6 == 6) if (!Rans64DecTail(&rans[w6], &optr, oend)) return 0;
    if (w7 == 7) if (!Rans64DecTail(&rans[w7], &optr, oend)) return 0;

#undef w0
#undef w1
#undef w2
#undef w3
#undef w4
#undef w5
#undef w6
#undef w7

    return ptr - data.udata();
}

// --------------------------------------------------------------------------

encoder_o2::encoder_o2() {
}

encoder_o2::encoder_o2(const freq_hist_o2::histogram_t& hist) {
    init(hist);
}

void encoder_o2::init(const freq_hist_o2::histogram_t& hist) {
    table_.resize(258 * 258 * 257 * 3);
    byte_t *cp = table_.data();
    size_t rle_i, i, rle_j, j;
    *cp++ = 1;  // version

    if (std::find_if(hist.o1_size, hist.o1_size + 256, [](uint64_t v) { return v > 0; }) == hist.o1_size + 256) {
        *cp++ = 0;  // ingore;
    }
    else {
        *cp++ = 1;  // begin
        for (rle_i = i = 0; i < 256; i++) {
            if (hist.o1_size[i] == 0)
                continue;
            // Store frequency table
            if (rle_i) {
                rle_i--;
            }
            else {
                *cp++ = i;
                // FIXME: could use order-0 statistics to observe which alphabet
                // symbols are present and base RLE on that ordering instead.
                if (i && hist.o1_size[i - 1]) {
                    for (rle_i = i + 1; rle_i < 256 && hist.o1_size[rle_i]; rle_i++)
                        ;
                    rle_i -= i + 1;
                    *cp++ = rle_i;
                }
            }
            if (std::find_if(hist.o2_size[i], hist.o2_size[i] + 256, [](uint64_t v) { return v > 0; }) == hist.o2_size[i] + 256) {
                *cp++ = 0;  // ingore;
            }
            else {
                *cp++ = 1;  // begin
                for (rle_j = j = 0; j < 256; j++) {
                    if (hist.o2_size[i][j] == 0)
                        continue;
                    // Store frequency table
                    if (rle_j) {
                        rle_j--;
                    }
                    else {
                        *cp++ = j;
                        // FIXME: could use order-0 statistics to observe which alphabet
                        // symbols are present and base RLE on that ordering instead.
                        if (j && hist.o2_size[i][j - 1]) {
                            for (rle_j = j + 1; rle_j < 256 && hist.o2_size[i][rle_j]; rle_j++)
                                ;
                            rle_j -= j + 1;
                            *cp++ = rle_j;
                        }
                    }
                    Rans64BuildCTable(&cp, hist.o2[i][j], syms_[i][j]);
                }
                *cp++ = 0;  // end
            }
            Rans64BuildCTable(&cp, hist.o1[i], syms_[256][i]);
        }
        *cp++ = 0;  // end
    }
    Rans64BuildCTable(&cp, hist.o0, syms_[256][256]);
    table_.risk_set_size(cp - table_.data());
    assert(table_.size() < 258 * 258 * 257 * 3);
    TerarkContext context;
    auto table_ans_bytes = rANS_static_64::encode_o1(fstring(table_).substr(1), &context);
    auto table_ans = table_ans_bytes.data;
    if (table_ans.size() < table_.size()) {
        table_[0] = 255;  // encoded
        memcpy(table_.data() + 1, table_ans.data(), table_ans.size());
        table_.risk_set_size(table_ans.size() + 1);
    }
    table_.shrink_to_fit();
}

const valvec<byte_t>& encoder_o2::table() const {
    return table_;
}

EntropyBytes encoder_o2::encode(fstring record, TerarkContext* context) const {
    if (record.size() < pow_t<RECORD_MAX_SIZE, 1>::value) {
        return encode_xN<1>(record, context, true);
    }
    else if (record.size() < pow_t<RECORD_MAX_SIZE, 2>::value) {
        return encode_xN<2>(record, context, true);
    }
    else if (record.size() < pow_t<RECORD_MAX_SIZE, 4>::value) {
        return encode_xN<4>(record, context, true);
    }
    else {
        return encode_xN<8>(record, context, true);
    }
}

EntropyBytes encoder_o2::encode_x1(fstring record, TerarkContext* context) const {
    return encode_xN<1>(record, context, false);
}

EntropyBytes encoder_o2::encode_x2(fstring record, TerarkContext* context) const {
    return encode_xN<2>(record, context, false);
}

EntropyBytes encoder_o2::encode_x4(fstring record, TerarkContext* context) const {
    return encode_xN<4>(record, context, false);
}

EntropyBytes encoder_o2::encode_x8(fstring record, TerarkContext* context) const {
    return encode_xN<8>(record, context, false);
}

template<size_t N>
EntropyBytes encoder_o2::encode_xN(fstring record, TerarkContext* context, bool check) const {
    if (record.size() >= pow_t<RECORD_MAX_SIZE, N>::value) {
        return {{ nullptr, ptrdiff_t(0) }, {}};
    }
    auto ctx_buffer = context->alloc();
    ctx_buffer.resize(record.size() * 5 / 4 + 8 * N + 8);

#define w7 (N == 8 ? 7 : 0)
#define w6 (N == 8 ? 6 : 0)
#define w5 (N == 8 ? 5 : 0)
#define w4 (N == 8 ? 4 : 0)
#define w3 (N >= 4 ? 3 : 0)
#define w2 (N >= 4 ? 2 : 0)
#define w1 (N >= 2 ? 1 : 0)
#define w0 0

    Rans64State rans[N];
    size_t size = record.size();
    const byte_t* end = record.udata() + size;

    if (w7 == 7) Rans64EncInit(&rans[w7], &end, &size);
    if (w6 == 6) Rans64EncInit(&rans[w6], &end, &size);
    if (w5 == 5) Rans64EncInit(&rans[w5], &end, &size);
    if (w4 == 4) Rans64EncInit(&rans[w4], &end, &size);
    if (w3 == 3) Rans64EncInit(&rans[w3], &end, &size);
    if (w2 == 2) Rans64EncInit(&rans[w2], &end, &size);
    if (w1 == 1) Rans64EncInit(&rans[w1], &end, &size);
    if (w0 == 0) Rans64EncInit(&rans[w0], &end, &size);

    byte_t* ptr = ctx_buffer.data() + ctx_buffer.size();
    const byte_t* record_data = record.udata();

    if (N > 1) {
        intptr_t b = (intptr_t)size / N * (N - 1) + 1;
        for (intptr_t i = (intptr_t)size - 1, e = (intptr_t)(size - size % N); i >= e; --i) {
            const Rans64EncSymbol* s = &syms_[i <= b ? 256 : record_data[i - 2]][i < b ? 256 : record_data[i - 1]][record_data[i]];
            Rans64EncRenorm(&rans[N - 1], &ptr, ctx_buffer, s->freq, TF_SHIFT);
            Rans64EncPutSymbol(&rans[N - 1], s, TF_SHIFT);
        }
    }

    if (size >= N) {
        byte_t ll[N], l[N];
        intptr_t i[N];

        if (w7 == 7) i[w7] = (intptr_t)size / N * 8 - 3;
        if (w6 == 6) i[w6] = (intptr_t)size / N * 7 - 3;
        if (w5 == 5) i[w5] = (intptr_t)size / N * 6 - 3;
        if (w4 == 4) i[w4] = (intptr_t)size / N * 5 - 3;
        if (w3 == 3) i[w3] = (intptr_t)size / N * 4 - 3;
        if (w2 == 2) i[w2] = (intptr_t)size / N * 3 - 3;
        if (w1 == 1) i[w1] = (intptr_t)size / N * 2 - 3;
        if (w0 == 0) i[w0] = (intptr_t)size / N * 1 - 3;

        const Rans64EncSymbol* s[N];
        if (size >= N * 2) {

            if (w7 == 7) l[w7] = record_data[i[w7] + 1];
            if (w6 == 6) l[w6] = record_data[i[w6] + 1];
            if (w5 == 5) l[w5] = record_data[i[w5] + 1];
            if (w4 == 4) l[w4] = record_data[i[w4] + 1];
            if (w3 == 3) l[w3] = record_data[i[w3] + 1];
            if (w2 == 2) l[w2] = record_data[i[w2] + 1];
            if (w1 == 1) l[w1] = record_data[i[w1] + 1];
            if (w0 == 0) l[w0] = record_data[i[w0] + 1];

            if (w7 == 7) ll[w7] = record_data[i[w7] + 2];
            if (w6 == 6) ll[w6] = record_data[i[w6] + 2];
            if (w5 == 5) ll[w5] = record_data[i[w5] + 2];
            if (w4 == 4) ll[w4] = record_data[i[w4] + 2];
            if (w3 == 3) ll[w3] = record_data[i[w3] + 2];
            if (w2 == 2) ll[w2] = record_data[i[w2] + 2];
            if (w1 == 1) ll[w1] = record_data[i[w1] + 2];
            if (w0 == 0) ll[w0] = record_data[i[w0] + 2];

            while (i[w0] >= 0) {
                byte_t c[N];

                if (w7 == 7) s[w7] = &syms_[c[w7] = record_data[i[w7]]][l[w7]][ll[w7]];
                if (w6 == 6) s[w6] = &syms_[c[w6] = record_data[i[w6]]][l[w6]][ll[w6]];
                if (w5 == 5) s[w5] = &syms_[c[w5] = record_data[i[w5]]][l[w5]][ll[w5]];
                if (w4 == 4) s[w4] = &syms_[c[w4] = record_data[i[w4]]][l[w4]][ll[w4]];
                if (w3 == 3) s[w3] = &syms_[c[w3] = record_data[i[w3]]][l[w3]][ll[w3]];
                if (w2 == 2) s[w2] = &syms_[c[w2] = record_data[i[w2]]][l[w2]][ll[w2]];
                if (w1 == 1) s[w1] = &syms_[c[w1] = record_data[i[w1]]][l[w1]][ll[w1]];
                if (w0 == 0) s[w0] = &syms_[c[w0] = record_data[i[w0]]][l[w0]][ll[w0]];

                if (w7 == 7) Rans64EncRenorm(&rans[w7], &ptr, ctx_buffer, s[w7]->freq, TF_SHIFT);
                if (w6 == 6) Rans64EncRenorm(&rans[w6], &ptr, ctx_buffer, s[w6]->freq, TF_SHIFT);
                if (w5 == 5) Rans64EncRenorm(&rans[w5], &ptr, ctx_buffer, s[w5]->freq, TF_SHIFT);
                if (w4 == 4) Rans64EncRenorm(&rans[w4], &ptr, ctx_buffer, s[w4]->freq, TF_SHIFT);
                if (w3 == 3) Rans64EncRenorm(&rans[w3], &ptr, ctx_buffer, s[w3]->freq, TF_SHIFT);
                if (w2 == 2) Rans64EncRenorm(&rans[w2], &ptr, ctx_buffer, s[w2]->freq, TF_SHIFT);
                if (w1 == 1) Rans64EncRenorm(&rans[w1], &ptr, ctx_buffer, s[w1]->freq, TF_SHIFT);
                if (w0 == 0) Rans64EncRenorm(&rans[w0], &ptr, ctx_buffer, s[w0]->freq, TF_SHIFT);

                if (w7 == 7) Rans64EncPutSymbol(&rans[w7], s[w7], TF_SHIFT);
                if (w6 == 6) Rans64EncPutSymbol(&rans[w6], s[w6], TF_SHIFT);
                if (w5 == 5) Rans64EncPutSymbol(&rans[w5], s[w5], TF_SHIFT);
                if (w4 == 4) Rans64EncPutSymbol(&rans[w4], s[w4], TF_SHIFT);
                if (w3 == 3) Rans64EncPutSymbol(&rans[w3], s[w3], TF_SHIFT);
                if (w2 == 2) Rans64EncPutSymbol(&rans[w2], s[w2], TF_SHIFT);
                if (w1 == 1) Rans64EncPutSymbol(&rans[w1], s[w1], TF_SHIFT);
                if (w0 == 0) Rans64EncPutSymbol(&rans[w0], s[w0], TF_SHIFT);

                if (w7 == 7) { ll[w7] = l[w7]; l[w7] = c[w7]; }
                if (w6 == 6) { ll[w6] = l[w6]; l[w6] = c[w6]; }
                if (w5 == 5) { ll[w5] = l[w5]; l[w5] = c[w5]; }
                if (w4 == 4) { ll[w4] = l[w4]; l[w4] = c[w4]; }
                if (w3 == 3) { ll[w3] = l[w3]; l[w3] = c[w3]; }
                if (w2 == 2) { ll[w2] = l[w2]; l[w2] = c[w2]; }
                if (w1 == 1) { ll[w1] = l[w1]; l[w1] = c[w1]; }
                if (w0 == 0) { ll[w0] = l[w0]; l[w0] = c[w0]; }

                if (w7 == 7) --i[w7];
                if (w6 == 6) --i[w6];
                if (w5 == 5) --i[w5];
                if (w4 == 4) --i[w4];
                if (w3 == 3) --i[w3];
                if (w2 == 2) --i[w2];
                if (w1 == 1) --i[w1];
                if (w0 == 0) --i[w0];
            }

            if (w7 == 7) s[w7] = &syms_[256][l[w7]][ll[w7]];
            if (w6 == 6) s[w6] = &syms_[256][l[w6]][ll[w6]];
            if (w5 == 5) s[w5] = &syms_[256][l[w5]][ll[w5]];
            if (w4 == 4) s[w4] = &syms_[256][l[w4]][ll[w4]];
            if (w3 == 3) s[w3] = &syms_[256][l[w3]][ll[w3]];
            if (w2 == 2) s[w2] = &syms_[256][l[w2]][ll[w2]];
            if (w1 == 1) s[w1] = &syms_[256][l[w1]][ll[w1]];
            if (w0 == 0) s[w0] = &syms_[256][l[w0]][ll[w0]];

            if (w7 == 7) Rans64EncRenorm(&rans[w7], &ptr, ctx_buffer, s[w7]->freq, TF_SHIFT);
            if (w6 == 6) Rans64EncRenorm(&rans[w6], &ptr, ctx_buffer, s[w6]->freq, TF_SHIFT);
            if (w5 == 5) Rans64EncRenorm(&rans[w5], &ptr, ctx_buffer, s[w5]->freq, TF_SHIFT);
            if (w4 == 4) Rans64EncRenorm(&rans[w4], &ptr, ctx_buffer, s[w4]->freq, TF_SHIFT);
            if (w3 == 3) Rans64EncRenorm(&rans[w3], &ptr, ctx_buffer, s[w3]->freq, TF_SHIFT);
            if (w2 == 2) Rans64EncRenorm(&rans[w2], &ptr, ctx_buffer, s[w2]->freq, TF_SHIFT);
            if (w1 == 1) Rans64EncRenorm(&rans[w1], &ptr, ctx_buffer, s[w1]->freq, TF_SHIFT);
            if (w0 == 0) Rans64EncRenorm(&rans[w0], &ptr, ctx_buffer, s[w0]->freq, TF_SHIFT);

            if (w7 == 7) Rans64EncPutSymbol(&rans[w7], s[w7], TF_SHIFT);
            if (w6 == 6) Rans64EncPutSymbol(&rans[w6], s[w6], TF_SHIFT);
            if (w5 == 5) Rans64EncPutSymbol(&rans[w5], s[w5], TF_SHIFT);
            if (w4 == 4) Rans64EncPutSymbol(&rans[w4], s[w4], TF_SHIFT);
            if (w3 == 3) Rans64EncPutSymbol(&rans[w3], s[w3], TF_SHIFT);
            if (w2 == 2) Rans64EncPutSymbol(&rans[w2], s[w2], TF_SHIFT);
            if (w1 == 1) Rans64EncPutSymbol(&rans[w1], s[w1], TF_SHIFT);
            if (w0 == 0) Rans64EncPutSymbol(&rans[w0], s[w0], TF_SHIFT);

        }
        else {

            if (w7 == 7) l[w7] = record_data[i[w7] + 2];
            if (w6 == 6) l[w6] = record_data[i[w6] + 2];
            if (w5 == 5) l[w5] = record_data[i[w5] + 2];
            if (w4 == 4) l[w4] = record_data[i[w4] + 2];
            if (w3 == 3) l[w3] = record_data[i[w3] + 2];
            if (w2 == 2) l[w2] = record_data[i[w2] + 2];
            if (w1 == 1) l[w1] = record_data[i[w1] + 2];
            if (w0 == 0) l[w0] = record_data[i[w0] + 2];

        }

        if (w7 == 7) s[w7] = &syms_[256][256][l[w7]];
        if (w6 == 6) s[w6] = &syms_[256][256][l[w6]];
        if (w5 == 5) s[w5] = &syms_[256][256][l[w5]];
        if (w4 == 4) s[w4] = &syms_[256][256][l[w4]];
        if (w3 == 3) s[w3] = &syms_[256][256][l[w3]];
        if (w2 == 2) s[w2] = &syms_[256][256][l[w2]];
        if (w1 == 1) s[w1] = &syms_[256][256][l[w1]];
        if (w0 == 0) s[w0] = &syms_[256][256][l[w0]];

        if (w7 == 7) Rans64EncRenorm(&rans[w7], &ptr, ctx_buffer, s[w7]->freq, TF_SHIFT);
        if (w6 == 6) Rans64EncRenorm(&rans[w6], &ptr, ctx_buffer, s[w6]->freq, TF_SHIFT);
        if (w5 == 5) Rans64EncRenorm(&rans[w5], &ptr, ctx_buffer, s[w5]->freq, TF_SHIFT);
        if (w4 == 4) Rans64EncRenorm(&rans[w4], &ptr, ctx_buffer, s[w4]->freq, TF_SHIFT);
        if (w3 == 3) Rans64EncRenorm(&rans[w3], &ptr, ctx_buffer, s[w3]->freq, TF_SHIFT);
        if (w2 == 2) Rans64EncRenorm(&rans[w2], &ptr, ctx_buffer, s[w2]->freq, TF_SHIFT);
        if (w1 == 1) Rans64EncRenorm(&rans[w1], &ptr, ctx_buffer, s[w1]->freq, TF_SHIFT);
        if (w0 == 0) Rans64EncRenorm(&rans[w0], &ptr, ctx_buffer, s[w0]->freq, TF_SHIFT);

        if (w7 == 7) Rans64EncPutSymbol(&rans[w7], s[w7], TF_SHIFT);
        if (w6 == 6) Rans64EncPutSymbol(&rans[w6], s[w6], TF_SHIFT);
        if (w5 == 5) Rans64EncPutSymbol(&rans[w5], s[w5], TF_SHIFT);
        if (w4 == 4) Rans64EncPutSymbol(&rans[w4], s[w4], TF_SHIFT);
        if (w3 == 3) Rans64EncPutSymbol(&rans[w3], s[w3], TF_SHIFT);
        if (w2 == 2) Rans64EncPutSymbol(&rans[w2], s[w2], TF_SHIFT);
        if (w1 == 1) Rans64EncPutSymbol(&rans[w1], s[w1], TF_SHIFT);
        if (w0 == 0) Rans64EncPutSymbol(&rans[w0], s[w0], TF_SHIFT);

    }

    size = record.size();

    if (w7 == 7) Rans64EncFlush(&rans[w7], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w6 == 6) Rans64EncFlush(&rans[w6], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w5 == 5) Rans64EncFlush(&rans[w5], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w4 == 4) Rans64EncFlush(&rans[w4], &ptr, ctx_buffer, &size, XCheck{ check, N > 8 });  // 1:0 2:0 4:0 8:0
    if (w3 == 3) Rans64EncFlush(&rans[w3], &ptr, ctx_buffer, &size, XCheck{ check, N > 4 });  // 1:0 2:0 4:0 8:1
    if (w2 == 2) Rans64EncFlush(&rans[w2], &ptr, ctx_buffer, &size, XCheck{ check, N > 4 });  // 1:0 2:0 4:0 8:1
    if (w1 == 1) Rans64EncFlush(&rans[w1], &ptr, ctx_buffer, &size, XCheck{ check, N > 2 });  // 1:0 2:0 4:1 8:1
    if (w0 == 0) Rans64EncFlush(&rans[w0], &ptr, ctx_buffer, &size, XCheck{ check, N > 1 });  // 1:0 2:1 4:1 8:1

    assert(size == 0);

#undef w7
#undef w6
#undef w5
#undef w4
#undef w3
#undef w2
#undef w1
#undef w0

    return EntropyBytes{
        fstring{ptr, ctx_buffer.data() + ctx_buffer.size() - ptr},
        std::move(ctx_buffer)
    };
}

// --------------------------------------------------------------------------

decoder_o2::decoder_o2() {
}

decoder_o2::decoder_o2(fstring table, size_t* psize) {
    init(table, psize);
}

void decoder_o2::init(fstring table, size_t* psize) {
    size_t i, rle_i, j, rle_j;
    const byte_t *cp = table.udata();
    const byte_t *end = cp + table.size();

    valvec<byte_t> table_ans;
    if (*cp++ == 255) {
        TerarkContext context;
        size_t read = rANS_static_64::decode_o1(table.substr(1), &table_ans, &context);
        if (psize != nullptr) {
            *psize = read + 1;
        }
        cp = table_ans.data();
        end = cp + table_ans.size();
    }
    Rans64DecSymbol syms[256];
    byte_t ari[TOTFREQ];

    if (*cp++ != 0) {
        rle_i = 0;
        i = *cp++;
        do {
            if (*cp++ != 0) {
                rle_j = 0;
                j = *cp++;
                do {
                    memset(syms, 0, sizeof syms);
                    Rans64BuildDTable(&cp, ari, syms);
                    convert_symbol(syms, &syms_[i][j]);
                    if (!rle_j && j + 1 == *cp) {
                        j = *cp++;
                        rle_j = *cp++;
                    }
                    else if (rle_j) {
                        rle_j--;
                        j++;
                    }
                    else {
                        j = *cp++;
                    }
                } while (j);
            }
            memset(syms, 0, sizeof syms);
            Rans64BuildDTable(&cp, ari, syms);
            convert_symbol(syms, &syms_[256][i]);
            if (!rle_i && i + 1 == *cp) {
                i = *cp++;
                rle_i = *cp++;
            }
            else if (rle_i) {
                rle_i--;
                i++;
            }
            else {
                i = *cp++;
            }
        } while (i);
    }
    memset(syms, 0, sizeof syms);
    Rans64BuildDTable(&cp, ari, syms);
    convert_symbol(syms, &syms_[256][256]);
    if (psize != nullptr && end == table.udata() + table.size()) {
        *psize = cp - table.udata();
    }
}

size_t decoder_o2::decode(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    size_t read;
    if ((read = decode_xN<8>(data, record, context, true)) > 0) {
        return read;
    }
    if ((read = decode_xN<4>(data, record, context, true)) > 0) {
        return read;
    }
    if ((read = decode_xN<2>(data, record, context, true)) > 0) {
        return read;
    }
    if ((read = decode_xN<1>(data, record, context, true)) > 0) {
        return read;
    }
    return 0;
}

size_t decoder_o2::decode_x1(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<1>(data, record, context, false);
}

size_t decoder_o2::decode_x2(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<2>(data, record, context, false);
}

size_t decoder_o2::decode_x4(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<4>(data, record, context, false);
}

size_t decoder_o2::decode_x8(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    return decode_xN<8>(data, record, context, false);
}

template<size_t N>
size_t decoder_o2::decode_xN(fstring data, valvec<byte_t>* record, TerarkContext* context, bool check) const {
    record->risk_set_size(0);
    if (data.size() < N * 2) {
        return 0;
    }

#define w0 0
#define w1 (N >= 2 ? 1 : 0)
#define w2 (N >= 4 ? 2 : 0)
#define w3 (N >= 4 ? 3 : 0)
#define w4 (N == 8 ? 4 : 0)
#define w5 (N == 8 ? 5 : 0)
#define w6 (N == 8 ? 6 : 0)
#define w7 (N == 8 ? 7 : 0)

    Rans64State rans[N];
    const byte_t* ptr = data.udata();
    const byte_t* end = data.udata() + data.size();
    size_t record_size = 0;

    if (w0 == 0) if (!Rans64DecInit(&rans[w0], &record_size, &ptr, end, XCheck{ check, N > 1 })) return 0;
    if (w1 == 1) if (!Rans64DecInit(&rans[w1], &record_size, &ptr, end, XCheck{ check, N > 2 })) return 0;
    if (w2 == 2) if (!Rans64DecInit(&rans[w2], &record_size, &ptr, end, XCheck{ check, N > 4 })) return 0;
    if (w3 == 3) if (!Rans64DecInit(&rans[w3], &record_size, &ptr, end, XCheck{ check, N > 4 })) return 0;
    if (w4 == 4) if (!Rans64DecInit(&rans[w4], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;
    if (w5 == 5) if (!Rans64DecInit(&rans[w5], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;
    if (w6 == 6) if (!Rans64DecInit(&rans[w6], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;
    if (w7 == 7) if (!Rans64DecInit(&rans[w7], &record_size, &ptr, end, XCheck{ check, N > 8 })) return 0;

    record->resize_no_init(record_size);
    size_t ll[N], l[N];
    byte_t* o[N];
    size_t size = record_size > N * (RANS_L_BITS / 8) ? record_size - N * (RANS_L_BITS / 8) : 0;

    if (w0 == 0) { l[w0] = 256; ll[w0] = 256; }
    if (w1 == 1) { l[w1] = 256; ll[w1] = 256; }
    if (w2 == 2) { l[w2] = 256; ll[w2] = 256; }
    if (w3 == 3) { l[w3] = 256; ll[w3] = 256; }
    if (w4 == 4) { l[w4] = 256; ll[w4] = 256; }
    if (w5 == 5) { l[w5] = 256; ll[w5] = 256; }
    if (w6 == 6) { l[w6] = 256; ll[w6] = 256; }
    if (w7 == 7) { l[w7] = 256; ll[w7] = 256; }

    if (w0 == 0) o[w0] = record->data() + size / N * 0;
    if (w1 == 1) o[w1] = record->data() + size / N * 1;
    if (w2 == 2) o[w2] = record->data() + size / N * 2;
    if (w3 == 3) o[w3] = record->data() + size / N * 3;
    if (w4 == 4) o[w4] = record->data() + size / N * 4;
    if (w5 == 5) o[w5] = record->data() + size / N * 5;
    if (w6 == 6) o[w6] = record->data() + size / N * 6;
    if (w7 == 7) o[w7] = record->data() + size / N * 7;

    for (size_t i = 0, e = size / N; i < e; ++i) {
        uint32_t m[N];
        byte_t c[N];

        if (w0 == 0) m[w0] = rans[w0] & (TOTFREQ - 1);
        if (w1 == 1) m[w1] = rans[w1] & (TOTFREQ - 1);
        if (w2 == 2) m[w2] = rans[w2] & (TOTFREQ - 1);
        if (w3 == 3) m[w3] = rans[w3] & (TOTFREQ - 1);
        if (w4 == 4) m[w4] = rans[w4] & (TOTFREQ - 1);
        if (w5 == 5) m[w5] = rans[w5] & (TOTFREQ - 1);
        if (w6 == 6) m[w6] = rans[w6] & (TOTFREQ - 1);
        if (w7 == 7) m[w7] = rans[w7] & (TOTFREQ - 1);

        if (w0 == 0) c[w0] = ari(&syms_[ll[w0]][l[w0]], m[w0]);
        if (w1 == 1) c[w1] = ari(&syms_[ll[w1]][l[w1]], m[w1]);
        if (w2 == 2) c[w2] = ari(&syms_[ll[w2]][l[w2]], m[w2]);
        if (w3 == 3) c[w3] = ari(&syms_[ll[w3]][l[w3]], m[w3]);
        if (w4 == 4) c[w4] = ari(&syms_[ll[w4]][l[w4]], m[w4]);
        if (w5 == 5) c[w5] = ari(&syms_[ll[w5]][l[w5]], m[w5]);
        if (w6 == 6) c[w6] = ari(&syms_[ll[w6]][l[w6]], m[w6]);
        if (w7 == 7) c[w7] = ari(&syms_[ll[w7]][l[w7]], m[w7]);

        if (w0 == 0) rans[w0] = syms_[ll[w0]][l[w0]].freq[c[w0]] * (rans[w0] >> TF_SHIFT);
        if (w1 == 1) rans[w1] = syms_[ll[w1]][l[w1]].freq[c[w1]] * (rans[w1] >> TF_SHIFT);
        if (w2 == 2) rans[w2] = syms_[ll[w2]][l[w2]].freq[c[w2]] * (rans[w2] >> TF_SHIFT);
        if (w3 == 3) rans[w3] = syms_[ll[w3]][l[w3]].freq[c[w3]] * (rans[w3] >> TF_SHIFT);
        if (w4 == 4) rans[w4] = syms_[ll[w4]][l[w4]].freq[c[w4]] * (rans[w4] >> TF_SHIFT);
        if (w5 == 5) rans[w5] = syms_[ll[w5]][l[w5]].freq[c[w5]] * (rans[w5] >> TF_SHIFT);
        if (w6 == 6) rans[w6] = syms_[ll[w6]][l[w6]].freq[c[w6]] * (rans[w6] >> TF_SHIFT);
        if (w7 == 7) rans[w7] = syms_[ll[w7]][l[w7]].freq[c[w7]] * (rans[w7] >> TF_SHIFT);

        if (w0 == 0) rans[w0] += m[w0] - syms_[ll[w0]][l[w0]].start[c[w0]];
        if (w1 == 1) rans[w1] += m[w1] - syms_[ll[w1]][l[w1]].start[c[w1]];
        if (w2 == 2) rans[w2] += m[w2] - syms_[ll[w2]][l[w2]].start[c[w2]];
        if (w3 == 3) rans[w3] += m[w3] - syms_[ll[w3]][l[w3]].start[c[w3]];
        if (w4 == 4) rans[w4] += m[w4] - syms_[ll[w4]][l[w4]].start[c[w4]];
        if (w5 == 5) rans[w5] += m[w5] - syms_[ll[w5]][l[w5]].start[c[w5]];
        if (w6 == 6) rans[w6] += m[w6] - syms_[ll[w6]][l[w6]].start[c[w6]];
        if (w7 == 7) rans[w7] += m[w7] - syms_[ll[w7]][l[w7]].start[c[w7]];

        if (w0 == 0) ll[w0] = l[w0];
        if (w1 == 1) ll[w1] = l[w1];
        if (w2 == 2) ll[w2] = l[w2];
        if (w3 == 3) ll[w3] = l[w3];
        if (w4 == 4) ll[w4] = l[w4];
        if (w5 == 5) ll[w5] = l[w5];
        if (w6 == 6) ll[w6] = l[w6];
        if (w7 == 7) ll[w7] = l[w7];

        if (w0 == 0) *o[w0]++ = l[w0] = c[w0];
        if (w1 == 1) *o[w1]++ = l[w1] = c[w1];
        if (w2 == 2) *o[w2]++ = l[w2] = c[w2];
        if (w3 == 3) *o[w3]++ = l[w3] = c[w3];
        if (w4 == 4) *o[w4]++ = l[w4] = c[w4];
        if (w5 == 5) *o[w5]++ = l[w5] = c[w5];
        if (w6 == 6) *o[w6]++ = l[w6] = c[w6];
        if (w7 == 7) *o[w7]++ = l[w7] = c[w7];

        if (w0 == 0) if (!Rans64DecRenorm(&rans[w0], &ptr, end)) return 0;
        if (w1 == 1) if (!Rans64DecRenorm(&rans[w1], &ptr, end)) return 0;
        if (w2 == 2) if (!Rans64DecRenorm(&rans[w2], &ptr, end)) return 0;
        if (w3 == 3) if (!Rans64DecRenorm(&rans[w3], &ptr, end)) return 0;
        if (w4 == 4) if (!Rans64DecRenorm(&rans[w4], &ptr, end)) return 0;
        if (w5 == 5) if (!Rans64DecRenorm(&rans[w5], &ptr, end)) return 0;
        if (w6 == 6) if (!Rans64DecRenorm(&rans[w6], &ptr, end)) return 0;
        if (w7 == 7) if (!Rans64DecRenorm(&rans[w7], &ptr, end)) return 0;
    }
    for (size_t i = 0, e = size % N; N > 1 && i < e; ++i) {
        uint32_t m = rans[N - 1] & (TOTFREQ - 1);
        byte_t c = ari(&syms_[ll[N - 1]][l[N - 1]], m);
        rans[N - 1] = syms_[ll[N - 1]][l[N - 1]].freq[c] * (rans[N - 1] >> TF_SHIFT);
        rans[N - 1] += m - syms_[ll[N - 1]][l[N - 1]].start[c];
        ll[N - 1] = l[N - 1];
        *o[N - 1]++ = l[N - 1] = c;
        if (!Rans64DecRenorm(&rans[N - 1], &ptr, end)) return 0;
    }

    byte_t* optr = record->data() + size;
    byte_t* oend = record->data() + record_size;

    if (w0 == 0) if (!Rans64DecTail(&rans[w0], &optr, oend)) return 0;
    if (w1 == 1) if (!Rans64DecTail(&rans[w1], &optr, oend)) return 0;
    if (w2 == 2) if (!Rans64DecTail(&rans[w2], &optr, oend)) return 0;
    if (w3 == 3) if (!Rans64DecTail(&rans[w3], &optr, oend)) return 0;
    if (w4 == 4) if (!Rans64DecTail(&rans[w4], &optr, oend)) return 0;
    if (w5 == 5) if (!Rans64DecTail(&rans[w5], &optr, oend)) return 0;
    if (w6 == 6) if (!Rans64DecTail(&rans[w6], &optr, oend)) return 0;
    if (w7 == 7) if (!Rans64DecTail(&rans[w7], &optr, oend)) return 0;

#undef w0
#undef w1
#undef w2
#undef w3
#undef w4
#undef w5
#undef w6
#undef w7

    return ptr - data.udata();
}

inline void decoder_o2::convert_symbol(const Rans64DecSymbol* from, Symbol* to) {
    for (size_t i = 0; i < 256; ++i) {
        to->start[i] = from[i].start;
        to->freq[i] = from[i].freq;
    }
    if (to->freq[255] == 0) {
        to->start[255] = 4096;
    }
    for (size_t i = 255; i > 0; ) {
        --i;
        if (to->freq[i] == 0) {
            to->start[i] = to->start[i + 1];
        }
    }
}

inline byte_t decoder_o2::ari(const Symbol* to, uint32_t m) {
    return upper_bound_0(to->start, 256, uint16_t(m)) - 1;
}

// --------------------------------------------------------------------------

}}
