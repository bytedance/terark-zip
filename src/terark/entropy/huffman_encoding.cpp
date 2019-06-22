#include "huffman_encoding.hpp"
#include <memory>
#include <cmath>
#include <terark/bitmanip.hpp>
#ifdef __BMI2__
#   include <terark/succinct/rank_select_inline_bmi2.hpp>
#else
#   include <terark/succinct/rank_select_inline_slow.hpp>
#endif
#ifdef __SSE4_2__
#   include <mmintrin.h>    //MMX
#   include <xmmintrin.h>   //SSE(include mmintrin.h)
#   include <emmintrin.h>   //SSE2(include xmmintrin.h)
#   include <pmmintrin.h>   //SSE3(include emmintrin.h)
#   include <tmmintrin.h>   //SSSE3(include pmmintrin.h)
#   include <smmintrin.h>   //SSE4.1(include tmmintrin.h)
#   include <nmmintrin.h>   //SSE4.2(include smmintrin.h)
#   include <immintrin.h>   //AVX2(include immintrin.h)
#endif

#if _MSC_VER && 0
#   define NOMINMAX
#   include <Windows.h>
#   undef assert
#   define assert(exp) do { if(!(exp)) DebugBreak(); } while(false)
#endif

namespace terark { namespace Huffman {

static constexpr size_t HEADER_BLOCK_BITS = 64 - BLOCK_BITS;

struct HuffmanState {
    uint64_t bits;
    size_t bit_count;
};

struct HuffmanEncBuildItem {
    uint32_t count;
    uint16_t parent;
    byte_t data;
    uint8_t bit_count;
};

// Put a symbol
static inline void HuffmanEncPutSymbol(HuffmanState* r, HuffmanEncSymbol sym) {
    assert(sym.bit_count > 0);

    r->bits = (r->bits >> sym.bit_count) | uint64_t(sym.bits) << (64 - sym.bit_count);
    r->bit_count += sym.bit_count;
}

// Initial , encode bits equal or less than BLOCK_BITS * N
template<class Buffer>
static inline bool HuffmanEncHeader(HuffmanState* r, size_t* pbit_count, HuffmanEncSymbol sym, size_t max_bit_count, EntropyBitsReverseWriter<Buffer>* writer) {
    if (sym.bit_count + *pbit_count > max_bit_count) {
        return false;
    }
    *pbit_count += sym.bit_count;
    HuffmanEncPutSymbol(r, sym);
    if (r->bit_count >= HEADER_BLOCK_BITS) {
        r->bit_count -= HEADER_BLOCK_BITS;
        writer->write(r->bits << r->bit_count, HEADER_BLOCK_BITS);
    }
    return true;
}

// Renormalise state , keep state.bit_count equal BLOCK_BITS
template<class Buffer>
static inline void HuffmanEncRenorm(HuffmanState* r, EntropyBitsReverseWriter<Buffer>* writer) {
    assert(r->bit_count > BLOCK_BITS);

    size_t bit_count = r->bit_count - BLOCK_BITS;
    writer->write(r->bits << BLOCK_BITS, bit_count);
    r->bit_count -= bit_count;
}

// Flush state
template<class Buffer>
static inline void HuffmanEncFlush(HuffmanState* r, EntropyBitsReverseWriter<Buffer>* writer) {
    if (r->bit_count > 0) {
        writer->write(r->bits, r->bit_count);
    }
}

// Read state from bits stream
static inline uint16_t HuffmanDecStateInit(EntropyBitsReader* reader) {
    assert(reader->size() > BLOCK_BITS);
    uint64_t bits = 0;
    size_t bit_count = 0;
    reader->read(BLOCK_BITS, &bits, &bit_count);
    return bits >> (64 - BLOCK_BITS);
}

// Renormalise state & find break point
template<class bits_t>
static inline bool HuffmanDecBreak(bits_t* pbits, size_t* pbit_count, size_t cnt, EntropyBitsReader* reader) {
    uint64_t bits = uint64_t(*pbits) << (64 - BLOCK_BITS + cnt);
    size_t bit_count = BLOCK_BITS - cnt;
    size_t read_bit_count = BLOCK_BITS - bit_count;
    bool ret;
    if ((ret = read_bit_count >= reader->size())) {
        read_bit_count = reader->size();
    }
    reader->read(read_bit_count, &bits, &bit_count);
    *pbits = bits >> (64 - BLOCK_BITS);
    *pbit_count = bit_count;
    return ret;
}

static void HuffmanBuildSort(HuffmanEncBuildItem* node, const uint64_t* count) {
    struct {
        uint32_t base;
        uint32_t current;
    } rank[32];
    uint32_t n;

    memset(rank, 0, sizeof(rank));
    for (n = 0; n < 256; n++) {
        uint32_t r = terark_bsr_u64(count[n] + 1);
        assert(r < 31);
        rank[r].base++;
    }
    for (n = 30; n > 0; n--) {
        rank[n - 1].base += rank[n].base;
    }
    for (n = 0; n < 32; n++) {
        rank[n].current = rank[n].base;
    }
    for (n = 0; n < 256; n++) {
        const uint32_t c = count[n];
        const uint32_t r = terark_bsr_u64(c + 1) + 1;
        uint32_t pos = rank[r].current++;
        while ((pos > rank[r].base) && (c > node[pos - 1].count)) {
            node[pos] = node[pos - 1], pos--;
        }
        node[pos].count = c;
        node[pos].data = (uint8_t)n;
    }
}


static void HuffmanBuildSetMaxHeight(HuffmanEncBuildItem* node, uint32_t last_not_null) {
    const uint32_t max_bit_count = node[last_not_null].bit_count;
    if (max_bit_count <= BLOCK_BITS) return;

    /* there are several too large elements (at least >= 2) */
    int64_t total_value = 0;
    const uint32_t base_value = 1 << (max_bit_count - BLOCK_BITS);
    uint32_t n = last_not_null;

    while (node[n].bit_count > BLOCK_BITS) {
        total_value += base_value - (1 << (max_bit_count - node[n].bit_count));
        node[n].bit_count = (uint8_t)BLOCK_BITS;
        n--;
    }                                                   /* n stops at node[n].nbBits <= BLOCK_BITS */
    while (node[n].bit_count == BLOCK_BITS) {
        n--;                                            /* n end at index of smallest symbol using < BLOCK_BITS */
    }

    /* renorm total_value */
    total_value >>= (max_bit_count - BLOCK_BITS);         /* note : total_value is necessarily a multiple of base_value */

    /* repay normalized cost */
    const uint32_t empty_symbol = 0xF0F0F0F0;
    uint32_t rank_last[BLOCK_BITS + 2];
    int pos;

    /* Get pos of last (smallest) symbol per rank */
    memset(rank_last, 0xF0, sizeof(rank_last));
    {
        uint32_t current_bit_count = BLOCK_BITS;
        for (pos = n; pos >= 0; pos--) {
            if (node[pos].bit_count >= current_bit_count) {
                continue;
            }
            current_bit_count = node[pos].bit_count;    /* < BLOCK_BITS */
            rank_last[BLOCK_BITS - current_bit_count] = pos;
        }
    }

    while (total_value > 0) {
        uint32_t bit_count_decrease = terark_bsr_u64(total_value) + 1;
        for (; bit_count_decrease > 1; bit_count_decrease--) {
            uint32_t high_pos = rank_last[bit_count_decrease];
            uint32_t low_pos = rank_last[bit_count_decrease - 1];
            if (high_pos == empty_symbol) {
                continue;
            }
            if (low_pos == empty_symbol) {
                break;
            }
            const uint32_t high_total = node[high_pos].count;
            const uint32_t low_total = 2 * node[low_pos].count;
            if (high_total <= low_total) {
                break;
            }
        }
        /* only triggered when no more rank 1 symbol left => find closest one (note : there is necessarily at least one !) */
        /* BLOCK_BITS test just to please gcc 5+; but it should not be necessary */
        while ((bit_count_decrease <= BLOCK_BITS) && (rank_last[bit_count_decrease] == empty_symbol)) {
            bit_count_decrease++;
        }
        total_value -= 1ll << (bit_count_decrease - 1);
        if (rank_last[bit_count_decrease - 1] == empty_symbol) {
            rank_last[bit_count_decrease - 1] = rank_last[bit_count_decrease];  /* this rank is no longer empty */
        }
        node[rank_last[bit_count_decrease]].bit_count++;
        if (rank_last[bit_count_decrease] == 0) {                               /* special case, reached largest symbol */
            rank_last[bit_count_decrease] = empty_symbol;
        }
        else {
            rank_last[bit_count_decrease]--;
            if (node[rank_last[bit_count_decrease]].bit_count != BLOCK_BITS - bit_count_decrease) {
                rank_last[bit_count_decrease] = empty_symbol;                   /* this rank is now empty */
            }
        }
    }

    while (total_value < 0) {   /* Sometimes, cost correction overshoot */
        /* special case : no rank 1 symbol (using BLOCK_BITS-1); let's create one from largest rank 0 (using BLOCK_BITS) */
        if (rank_last[1] == empty_symbol) {
            while (node[n].bit_count == BLOCK_BITS) {
                n--;
            }
            node[n + 1].bit_count--;
            rank_last[1] = n + 1;
            total_value++;
            continue;
        }
        node[rank_last[1] + 1].bit_count--;
        rank_last[1]++;
        total_value++;
    }
}

// Build CTable from freq table
static inline void HuffmanBuildCTable(byte_t** pptr, const uint64_t* freq, HuffmanEncSymbol* syms) {
    HuffmanEncBuildItem node_huffer[512 + 2];
    HuffmanEncBuildItem* node = node_huffer + 1;
    uint32_t n, null_rank = 255;
    int32_t low_S, low_N;
    uint16_t node_Nb = 256;
    uint32_t node_root;

    /* safety checks */
    memset(node_huffer, 0, sizeof(node_huffer));
    memset(syms, 0, sizeof(*syms) * 256);

    /* sort, decreasing order */
    HuffmanBuildSort(node, freq);

    /* init for parents */
    while (node[null_rank].count == 0) {
        null_rank--;
    }
    low_S = null_rank;
    node_root = node_Nb + low_S - 1;
    low_N = node_Nb;
    node[node_Nb].count = node[low_S].count + node[low_S - 1].count;
    node[low_S].parent = node[low_S - 1].parent = node_Nb;
    node_Nb++; low_S -= 2;
    for (n = node_Nb; n <= node_root; n++) {
        node[n].count = (uint32_t)(1U << 30);
    }
    node_huffer[0].count = (uint32_t)(1U << 31);

    /* create parents */
    while (node_Nb <= node_root) {
        uint32_t n1 = (node[low_S].count < node[low_N].count) ? low_S-- : low_N++;
        uint32_t n2 = (node[low_S].count < node[low_N].count) ? low_S-- : low_N++;
        node[node_Nb].count = node[n1].count + node[n2].count;
        node[n1].parent = node[n2].parent = node_Nb;
        node_Nb++;
    }

    /* distribute weights (unlimited tree height) */
    node[node_root].bit_count = 0;
    for (n = node_root - 1; n >= 256; n--) {
        node[n].bit_count = node[node[n].parent].bit_count + 1;
    }
    for (n = 0; n <= null_rank; n++) {
        node[n].bit_count = node[node[n].parent].bit_count + 1;
    }

    /* enforce maxTableLog */
    HuffmanBuildSetMaxHeight(node, null_rank);

    /* fill result into tree (val, nbBits) */
    uint16_t bit_count_per_rank[BLOCK_BITS + 1] = { 0 };
    uint16_t bits_pre_rank[BLOCK_BITS + 1] = { 0 };
    for (n = 0; n <= null_rank; n++) {
        bit_count_per_rank[node[n].bit_count]++;
    }
    /* determine stating value per rank */
    uint16_t min = 0;
    for (n = BLOCK_BITS; n > 0; n--) {
        bits_pre_rank[n] = min;                             /* get starting value within each rank */
        min += bit_count_per_rank[n];
        min >>= 1;
    }
    for (n = 0; n < 256; ++n) {
        syms[node[n].data].bit_count = node[n].bit_count;   /* push nbBits per symbol, symbol order */
    }
    for (n = 0; n < 256; ++n) {
        syms[n].bits = bits_pre_rank[syms[n].bit_count]++;  /* assign value within rank, symbol order */
    }
    if (!pptr) {
        return;
    }
    size_t rle_j, j;
    byte_t *&cp = *pptr;
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
        }
    }
    *cp++ = 0;  // end
}

// Build DTable from CTable
inline void HuffmanBuildDTable(const HuffmanEncSymbol* csyms, byte_t* ari, uint8_t* cnt) {
    struct item_t {
        uint16_t bits;
        uint8_t bit_count;
        uint8_t data;
    } item[256];
    size_t item_size = 0;
    for (size_t j = 0; j < 256; ++j) {
        if (csyms[j].bit_count == 0) {
            continue;
        }
        auto& ref = item[item_size++];
        ref.bits = uint16_t(csyms[j].bits) << (BLOCK_BITS - csyms[j].bit_count);
        ref.bit_count = csyms[j].bit_count;
        ref.data = j;
    }
    if (item_size == 0) {
        return;
    }
    std::sort(item, item + item_size, [](const item_t& l, const item_t& r) {
        return l.bits < r.bits;
    });
    size_t max_j = item_size - 1;
    for (size_t j = 0; j < max_j; ++j) {
        cnt[item[j].data] = item[j].bit_count;
        memset(&ari[item[j].bits], item[j].data, item[j + 1].bits - item[j].bits);
    }
    cnt[item[max_j].data] = item[max_j].bit_count;
    memset(&ari[item[max_j].bits], item[max_j].data, (1u << BLOCK_BITS) - item[max_j].bits);
}

// Build DTable from byte stream
inline void HuffmanBuildDTable(const byte_t** pptr, byte_t* ari, uint8_t* cnt) {
    HuffmanEncSymbol ctable[256];
    uint64_t freq[256];
    memset(&ctable, 0, sizeof ctable);
    memset(&freq, 0, sizeof freq);
    size_t j, rle_j = 0;
    if (pptr != nullptr) {
        const byte_t *&cp = *pptr;
        j = *cp++;
        do {
            size_t F;
            if ((F = *cp++) >= 128) {
                F &= ~128;
                F = ((F & 127) << 8) | *cp++;
            }
            freq[j] = F;

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
        HuffmanBuildCTable(nullptr, freq, ctable);
        HuffmanBuildDTable(ctable, ari, cnt);
    }
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
    if (hist.o0_size == 0) {
        *cp++ = 0;  // empty
        memset(syms_, 0, sizeof syms_);
    }
    else {
        *cp++ = 1;  // version
        HuffmanBuildCTable(&cp, hist.o0, syms_);
    }
    table_.risk_set_size(cp - table_.data());
    table_.shrink_to_fit();
}

const valvec<byte_t>& encoder::table() const {
    return table_;
}

void encoder::take_table(valvec<byte_t>* ptr) {
    *ptr = std::move(table_);
}

EntropyBytes encoder::encode(fstring record, TerarkContext* context) const {
    auto bits = bitwise_encode(record, context);
    return EntropyBitsToBytes(&bits);
}

EntropyBits encoder::bitwise_encode(fstring record, TerarkContext* context) const {
    auto ctx_buffer = context->alloc();
    ctx_buffer.ensure_capacity(record.size() * 5 / 4 + 8);
    const byte_t* data = record.udata();
    EntropyBitsReverseWriter<ContextBuffer> writer(&ctx_buffer);
    HuffmanState huf = { (uint64_t)0, (size_t)0 };
    for (intptr_t i = (intptr_t)record.size() - 1; i >= 0; --i) {
        HuffmanEncPutSymbol(&huf, syms_[data[i]]);
        if (huf.bit_count >= HEADER_BLOCK_BITS) {
            huf.bit_count -= HEADER_BLOCK_BITS;
            writer.write(huf.bits << huf.bit_count, HEADER_BLOCK_BITS);
        }
    }
    HuffmanEncFlush(&huf, &writer);
    return writer.finish(&ctx_buffer);
}

// --------------------------------------------------------------------------

decoder::decoder() {
}

decoder::decoder(fstring table, size_t* psize) {
    init(table, psize);
}

void decoder::init(fstring table, size_t* psize) {
    const byte_t *cp = table.udata();

    memset(&ari_, 0, sizeof ari_);
    memset(&cnt_, 255, sizeof cnt_);

    if (*cp == 0) {
        if (psize != nullptr) {
            *psize = 1;
        }
        return;
    }
    if (*cp != 1) {
        // unknow version
        abort();
    }
    ++cp;
    HuffmanBuildDTable(&cp, ari_, cnt_);

    if (psize != nullptr) {
        *psize = cp - table.udata();
    }
}

bool decoder::decode(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    auto bits = EntropyBytesToBits(data);
    return bitwise_decode(bits, record, context);
}

bool decoder::bitwise_decode(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const {
    record->risk_set_size(0);

    EntropyBitsReader reader(data);
    HuffmanState huf;
    if (terark_likely(reader.size() > 0)) {
        huf.bit_count = 0;
        huf.bits = 0;
        reader.read((reader.size() - 1) % HEADER_BLOCK_BITS + 1, &huf.bits, &huf.bit_count);
        record->ensure_capacity(HEADER_BLOCK_BITS);
        while (true) {
            if (huf.bit_count < BLOCK_BITS) {
                if (terark_likely(reader.size() > 0)) {
                    reader.read(HEADER_BLOCK_BITS, &huf.bits, &huf.bit_count);
                    record->ensure_capacity(record->size() + HEADER_BLOCK_BITS);
                }
                else if (huf.bit_count == 0) {
                    break;
                }
            }
            byte_t c = ari_[huf.bits >> (64 - BLOCK_BITS)];
            uint8_t b = cnt_[c];
            if (terark_unlikely(b > huf.bit_count)) return false;
            record->unchecked_push(c);
            huf.bits <<= b;
            huf.bit_count -= b;
        }
    }
    return true;
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
    if (hist.o0_size == 0) {
        *cp++ = 0;  // empty
        memset(syms_, 0, sizeof syms_);
    }
    else {
        *cp++ = 1;  // version

        if (std::find_if(hist.o1_size, hist.o1_size + 256, [](uint64_t v) { return v > 0; }) == hist.o1_size + 256) {
            *cp++ = 0;  // ingore;
            memset(syms_, 0, sizeof(HuffmanEncSymbol) * 256);
        }
        else {
            *cp++ = 1;  // begin
            for (rle_i = i = 0; i < 256; i++) {
                if (hist.o1_size[i] == 0) {
                    memset(syms_[i], 0, sizeof syms_[i]);
                    continue;
                }
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
                HuffmanBuildCTable(&cp, hist.o1[i], syms_[i]);
            }
            *cp++ = 0;  // end
        }
        HuffmanBuildCTable(&cp, hist.o0, syms_[256]);
    }
    table_.risk_set_size(cp - table_.data());
    assert(table_.size() < 258 * 257 * 3);
    freq_hist h;
    h.add_record(table_);
    h.finish();
    if (h.histogram().o0_size >= NORMALISE) {
        h.normalise(NORMALISE);
    }
    encoder e(h.histogram());
    auto table_huf_bytes = e.encode(table_, GetTlsTerarkContext());
    auto& table_huf = table_huf_bytes.data;
    if (table_huf.size() + e.table().size() + 1 < table_.size()) {
        table_[0] = 255;  // encoded
        size_t offset = 1;
        memcpy(table_.data() + offset, e.table().data(), e.table().size());
        offset += e.table().size();
        memcpy(table_.data() + offset, table_huf.data(), table_huf.size());
        offset += table_huf.size();
        table_.risk_set_size(offset);
    }
    table_.shrink_to_fit();
}

const valvec<byte_t>& encoder_o1::table() const {
    return table_;
}

void encoder_o1::take_table(valvec<byte_t>* ptr) {
    *ptr = std::move(table_);
}

EntropyBytes encoder_o1::encode_x1(fstring record, TerarkContext* context) const {
    auto bits = bitwise_encode_x1(record, context);
    return EntropyBitsToBytes(&bits);
}

EntropyBytes encoder_o1::encode_x2(fstring record, TerarkContext* context) const {
    auto bits = bitwise_encode_x2(record, context);
    return EntropyBitsToBytes(&bits);
}

EntropyBytes encoder_o1::encode_x4(fstring record, TerarkContext* context) const {
    auto bits = bitwise_encode_x4(record, context);
    return EntropyBitsToBytes(&bits);
}

EntropyBytes encoder_o1::encode_x8(fstring record, TerarkContext* context) const {
    auto bits = bitwise_encode_x8(record, context);
    return EntropyBitsToBytes(&bits);
}

EntropyBits encoder_o1::bitwise_encode_x1(fstring record, TerarkContext* context) const {
    auto ctx_buffer = context->alloc();
    ctx_buffer.ensure_capacity(record.size() * 5 / 4 + 8);
    const byte_t* data = record.udata();
    EntropyBitsReverseWriter<ContextBuffer> writer(&ctx_buffer);
    HuffmanState huf = { (uint64_t)0, (size_t)0 };
    for (intptr_t i = (intptr_t)record.size() - 1; i >= 0; --i) {
        HuffmanEncPutSymbol(&huf, syms_[i == 0 ? 256 : data[i - 1]][data[i]]);
        if (huf.bit_count >= HEADER_BLOCK_BITS) {
            huf.bit_count -= HEADER_BLOCK_BITS;
            writer.write(huf.bits << huf.bit_count, HEADER_BLOCK_BITS);
        }
    }
    HuffmanEncFlush(&huf, &writer);
    return writer.finish(&ctx_buffer);
}

EntropyBits encoder_o1::bitwise_encode_x2(fstring record, TerarkContext* context) const {
    return bitwise_encode_xN<2>(record, context);
}

EntropyBits encoder_o1::bitwise_encode_x4(fstring record, TerarkContext* context) const {
    return bitwise_encode_xN<4>(record, context);
}

EntropyBits encoder_o1::bitwise_encode_x8(fstring record, TerarkContext* context) const {
    return bitwise_encode_xN<8>(record, context);
}

template<size_t N>
EntropyBits encoder_o1::bitwise_encode_xN(fstring record, TerarkContext* context) const {
    auto ctx_buffer = context->alloc();
    ctx_buffer.ensure_capacity(record.size() * 5 / 4 + N * 8);

#define w7 (N >= 8 ? 7 : 0)
#define w6 (N >= 8 ? 6 : 0)
#define w5 (N >= 8 ? 5 : 0)
#define w4 (N >= 8 ? 4 : 0)
#define w3 (N >= 4 ? 3 : 0)
#define w2 (N >= 4 ? 2 : 0)
#define w1 (N >= 2 ? 1 : 0)
#define w0 0

    const byte_t* data = record.udata();
    size_t record_size = record.size();
    HuffmanState huf_init = { (uint64_t)0, (size_t)0 };

    EntropyBitsReverseWriter<ContextBuffer> writer(&ctx_buffer);
    intptr_t i[N], e[N];

    if (w0 == 0) i[w0] = (intptr_t)record_size / N + (0 < record_size % N) - 1;
    if (w1 == 1) i[w1] = (intptr_t)record_size / N + (1 < record_size % N) + i[w0];
    if (w2 == 2) i[w2] = (intptr_t)record_size / N + (2 < record_size % N) + i[w1];
    if (w3 == 3) i[w3] = (intptr_t)record_size / N + (3 < record_size % N) + i[w2];
    if (w4 == 4) i[w4] = (intptr_t)record_size / N + (4 < record_size % N) + i[w3];
    if (w5 == 5) i[w5] = (intptr_t)record_size / N + (5 < record_size % N) + i[w4];
    if (w6 == 6) i[w6] = (intptr_t)record_size / N + (6 < record_size % N) + i[w5];
    if (w7 == 7) i[w7] = (intptr_t)record_size / N + (7 < record_size % N) + i[w6];

    assert(i[N - 1] == (intptr_t)record_size - 1);

    if (w7 == 7) e[w7] = i[w6] + 1;
    if (w6 == 6) e[w6] = i[w5] + 1;
    if (w5 == 5) e[w5] = i[w4] + 1;
    if (w4 == 4) e[w4] = i[w3] + 1;
    if (w3 == 3) e[w3] = i[w2] + 1;
    if (w2 == 2) e[w2] = i[w1] + 1;
    if (w1 == 1) e[w1] = i[w0] + 1;
    if (w0 == 0) e[w0] = 0;

    size_t remain = N;

    do {
        intptr_t diff = i[N - 1] - e[N -1];
        size_t bit_count = 0;

        if (w7 == 7 && i[w7] - e[w7] > diff) { if (HuffmanEncHeader(&huf_init, &bit_count, syms_[i[w7] == e[w7] ? 256 : data[i[w7] - 1]][data[i[w7]]], BLOCK_BITS * N, &writer)) --i[w7]; else { remain = 7; break; } }
        if (w6 == 6 && i[w6] - e[w6] > diff) { if (HuffmanEncHeader(&huf_init, &bit_count, syms_[i[w6] == e[w6] ? 256 : data[i[w6] - 1]][data[i[w6]]], BLOCK_BITS * N, &writer)) --i[w6]; else { remain = 6; break; } }
        if (w5 == 5 && i[w5] - e[w5] > diff) { if (HuffmanEncHeader(&huf_init, &bit_count, syms_[i[w5] == e[w5] ? 256 : data[i[w5] - 1]][data[i[w5]]], BLOCK_BITS * N, &writer)) --i[w5]; else { remain = 5; break; } }
        if (w4 == 4 && i[w4] - e[w4] > diff) { if (HuffmanEncHeader(&huf_init, &bit_count, syms_[i[w4] == e[w4] ? 256 : data[i[w4] - 1]][data[i[w4]]], BLOCK_BITS * N, &writer)) --i[w4]; else { remain = 4; break; } }
        if (w3 == 3 && i[w3] - e[w3] > diff) { if (HuffmanEncHeader(&huf_init, &bit_count, syms_[i[w3] == e[w3] ? 256 : data[i[w3] - 1]][data[i[w3]]], BLOCK_BITS * N, &writer)) --i[w3]; else { remain = 3; break; } }
        if (w2 == 2 && i[w2] - e[w2] > diff) { if (HuffmanEncHeader(&huf_init, &bit_count, syms_[i[w2] == e[w2] ? 256 : data[i[w2] - 1]][data[i[w2]]], BLOCK_BITS * N, &writer)) --i[w2]; else { remain = 2; break; } }
        if (w1 == 1 && i[w1] - e[w1] > diff) { if (HuffmanEncHeader(&huf_init, &bit_count, syms_[i[w1] == e[w1] ? 256 : data[i[w1] - 1]][data[i[w1]]], BLOCK_BITS * N, &writer)) --i[w1]; else { remain = 1; break; } }
        if (w0 == 0 && i[w0] - e[w0] > diff) { if (HuffmanEncHeader(&huf_init, &bit_count, syms_[i[w0] == e[w0] ? 256 : data[i[w0] - 1]][data[i[w0]]], BLOCK_BITS * N, &writer)) --i[w0]; else { remain = 0; break; } }

        while (i[0] >= e[0]) {
            HuffmanEncSymbol s[N];

            if (w7 == 7) s[w7] = syms_[i[w7] == e[w7] ? 256 : data[i[w7] - 1]][data[i[w7]]];
            if (w6 == 6) s[w6] = syms_[i[w6] == e[w6] ? 256 : data[i[w6] - 1]][data[i[w6]]];
            if (w5 == 5) s[w5] = syms_[i[w5] == e[w5] ? 256 : data[i[w5] - 1]][data[i[w5]]];
            if (w4 == 4) s[w4] = syms_[i[w4] == e[w4] ? 256 : data[i[w4] - 1]][data[i[w4]]];
            if (w3 == 3) s[w3] = syms_[i[w3] == e[w3] ? 256 : data[i[w3] - 1]][data[i[w3]]];
            if (w2 == 2) s[w2] = syms_[i[w2] == e[w2] ? 256 : data[i[w2] - 1]][data[i[w2]]];
            if (w1 == 1) s[w1] = syms_[i[w1] == e[w1] ? 256 : data[i[w1] - 1]][data[i[w1]]];
            if (w0 == 0) s[w0] = syms_[i[w0] == e[w0] ? 256 : data[i[w0] - 1]][data[i[w0]]];

            if (w7 == 7) { if (HuffmanEncHeader(&huf_init, &bit_count, s[w7], BLOCK_BITS * N, &writer)) --i[w7]; else { remain = 7; break; } }
            if (w6 == 6) { if (HuffmanEncHeader(&huf_init, &bit_count, s[w6], BLOCK_BITS * N, &writer)) --i[w6]; else { remain = 6; break; } }
            if (w5 == 5) { if (HuffmanEncHeader(&huf_init, &bit_count, s[w5], BLOCK_BITS * N, &writer)) --i[w5]; else { remain = 5; break; } }
            if (w4 == 4) { if (HuffmanEncHeader(&huf_init, &bit_count, s[w4], BLOCK_BITS * N, &writer)) --i[w4]; else { remain = 4; break; } }
            if (w3 == 3) { if (HuffmanEncHeader(&huf_init, &bit_count, s[w3], BLOCK_BITS * N, &writer)) --i[w3]; else { remain = 3; break; } }
            if (w2 == 2) { if (HuffmanEncHeader(&huf_init, &bit_count, s[w2], BLOCK_BITS * N, &writer)) --i[w2]; else { remain = 2; break; } }
            if (w1 == 1) { if (HuffmanEncHeader(&huf_init, &bit_count, s[w1], BLOCK_BITS * N, &writer)) --i[w1]; else { remain = 1; break; } }
            if (w0 == 0) { if (HuffmanEncHeader(&huf_init, &bit_count, s[w0], BLOCK_BITS * N, &writer)) --i[w0]; else { remain = 0; break; } }
        }
    } while (false);

    HuffmanEncFlush(&huf_init, &writer);


    if (remain != N) {
        EntropyBitsReader reader(writer.finish(nullptr));
        HuffmanState huf[N];
        memset(huf, 0, sizeof huf);

        if (w7 == 7 && remain == 7) reader.read((reader.size() - 1) % BLOCK_BITS + 1, &huf[w7].bits, &huf[w7].bit_count);
        if (w6 == 6 && remain == 6) reader.read((reader.size() - 1) % BLOCK_BITS + 1, &huf[w6].bits, &huf[w6].bit_count);
        if (w5 == 5 && remain == 5) reader.read((reader.size() - 1) % BLOCK_BITS + 1, &huf[w5].bits, &huf[w5].bit_count);
        if (w4 == 4 && remain == 4) reader.read((reader.size() - 1) % BLOCK_BITS + 1, &huf[w4].bits, &huf[w4].bit_count);
        if (w3 == 3 && remain == 3) reader.read((reader.size() - 1) % BLOCK_BITS + 1, &huf[w3].bits, &huf[w3].bit_count);
        if (w2 == 2 && remain == 2) reader.read((reader.size() - 1) % BLOCK_BITS + 1, &huf[w2].bits, &huf[w2].bit_count);
        if (w1 == 1 && remain == 1) reader.read((reader.size() - 1) % BLOCK_BITS + 1, &huf[w1].bits, &huf[w1].bit_count);
        if (w0 == 0 && remain == 0) reader.read((reader.size() - 1) % BLOCK_BITS + 1, &huf[w0].bits, &huf[w0].bit_count);

        if (w7 == 7 && remain != 7) reader.read(BLOCK_BITS, &huf[w7].bits, &huf[w7].bit_count);
        if (w6 == 6 && remain != 6) reader.read(BLOCK_BITS, &huf[w6].bits, &huf[w6].bit_count);
        if (w5 == 5 && remain != 5) reader.read(BLOCK_BITS, &huf[w5].bits, &huf[w5].bit_count);
        if (w4 == 4 && remain != 4) reader.read(BLOCK_BITS, &huf[w4].bits, &huf[w4].bit_count);
        if (w3 == 3 && remain != 3) reader.read(BLOCK_BITS, &huf[w3].bits, &huf[w3].bit_count);
        if (w2 == 2 && remain != 2) reader.read(BLOCK_BITS, &huf[w2].bits, &huf[w2].bit_count);
        if (w1 == 1 && remain != 1) reader.read(BLOCK_BITS, &huf[w1].bits, &huf[w1].bit_count);
        if (w0 == 0 && remain != 0) reader.read(BLOCK_BITS, &huf[w0].bits, &huf[w0].bit_count);

        assert(reader.size() == 0);

        writer.reset();
        intptr_t diff = i[N - 1] - e[N - 1];

        if (w7 == 7) if (i[w7] - e[w7] > diff) { assert(remain >= 7); HuffmanEncPutSymbol(&huf[w7], syms_[i[w7] == e[w7] ? 256 : data[i[w7] - 1]][data[i[w7]]]); HuffmanEncRenorm(&huf[w7], &writer); --i[w7]; }
        if (w6 == 6) if (i[w6] - e[w6] > diff) { assert(remain >= 6); HuffmanEncPutSymbol(&huf[w6], syms_[i[w6] == e[w6] ? 256 : data[i[w6] - 1]][data[i[w6]]]); HuffmanEncRenorm(&huf[w6], &writer); --i[w6]; }
        if (w5 == 5) if (i[w5] - e[w5] > diff) { assert(remain >= 5); HuffmanEncPutSymbol(&huf[w5], syms_[i[w5] == e[w5] ? 256 : data[i[w5] - 1]][data[i[w5]]]); HuffmanEncRenorm(&huf[w5], &writer); --i[w5]; }
        if (w4 == 4) if (i[w4] - e[w4] > diff) { assert(remain >= 4); HuffmanEncPutSymbol(&huf[w4], syms_[i[w4] == e[w4] ? 256 : data[i[w4] - 1]][data[i[w4]]]); HuffmanEncRenorm(&huf[w4], &writer); --i[w4]; }
        if (w3 == 3) if (i[w3] - e[w3] > diff) { assert(remain >= 3); HuffmanEncPutSymbol(&huf[w3], syms_[i[w3] == e[w3] ? 256 : data[i[w3] - 1]][data[i[w3]]]); HuffmanEncRenorm(&huf[w3], &writer); --i[w3]; }
        if (w2 == 2) if (i[w2] - e[w2] > diff) { assert(remain >= 2); HuffmanEncPutSymbol(&huf[w2], syms_[i[w2] == e[w2] ? 256 : data[i[w2] - 1]][data[i[w2]]]); HuffmanEncRenorm(&huf[w2], &writer); --i[w2]; }
        if (w1 == 1) if (i[w1] - e[w1] > diff) { assert(remain >= 1); HuffmanEncPutSymbol(&huf[w1], syms_[i[w1] == e[w1] ? 256 : data[i[w1] - 1]][data[i[w1]]]); HuffmanEncRenorm(&huf[w1], &writer); --i[w1]; }
        if (w0 == 0) if (i[w0] - e[w0] > diff) { assert(remain >= 0); HuffmanEncPutSymbol(&huf[w0], syms_[i[w0] == e[w0] ? 256 : data[i[w0] - 1]][data[i[w0]]]); HuffmanEncRenorm(&huf[w0], &writer); --i[w0]; }

        while (i[0] >= e[0]) {
            HuffmanEncSymbol s[N];

            if (w7 == 7) s[w7] = syms_[i[w7] == e[w7] ? 256 : data[i[w7] - 1]][data[i[w7]]];
            if (w6 == 6) s[w6] = syms_[i[w6] == e[w6] ? 256 : data[i[w6] - 1]][data[i[w6]]];
            if (w5 == 5) s[w5] = syms_[i[w5] == e[w5] ? 256 : data[i[w5] - 1]][data[i[w5]]];
            if (w4 == 4) s[w4] = syms_[i[w4] == e[w4] ? 256 : data[i[w4] - 1]][data[i[w4]]];
            if (w3 == 3) s[w3] = syms_[i[w3] == e[w3] ? 256 : data[i[w3] - 1]][data[i[w3]]];
            if (w2 == 2) s[w2] = syms_[i[w2] == e[w2] ? 256 : data[i[w2] - 1]][data[i[w2]]];
            if (w1 == 1) s[w1] = syms_[i[w1] == e[w1] ? 256 : data[i[w1] - 1]][data[i[w1]]];
            if (w0 == 0) s[w0] = syms_[i[w0] == e[w0] ? 256 : data[i[w0] - 1]][data[i[w0]]];

            if (w7 == 7) HuffmanEncPutSymbol(&huf[w7], s[w7]);
            if (w6 == 6) HuffmanEncPutSymbol(&huf[w6], s[w6]);
            if (w5 == 5) HuffmanEncPutSymbol(&huf[w5], s[w5]);
            if (w4 == 4) HuffmanEncPutSymbol(&huf[w4], s[w4]);
            if (w3 == 3) HuffmanEncPutSymbol(&huf[w3], s[w3]);
            if (w2 == 2) HuffmanEncPutSymbol(&huf[w2], s[w2]);
            if (w1 == 1) HuffmanEncPutSymbol(&huf[w1], s[w1]);
            if (w0 == 0) HuffmanEncPutSymbol(&huf[w0], s[w0]);

            if (w7 == 7) HuffmanEncRenorm(&huf[w7], &writer);
            if (w6 == 6) HuffmanEncRenorm(&huf[w6], &writer);
            if (w5 == 5) HuffmanEncRenorm(&huf[w5], &writer);
            if (w4 == 4) HuffmanEncRenorm(&huf[w4], &writer);
            if (w3 == 3) HuffmanEncRenorm(&huf[w3], &writer);
            if (w2 == 2) HuffmanEncRenorm(&huf[w2], &writer);
            if (w1 == 1) HuffmanEncRenorm(&huf[w1], &writer);
            if (w0 == 0) HuffmanEncRenorm(&huf[w0], &writer);

            if (w7 == 7) --i[w7];
            if (w6 == 6) --i[w6];
            if (w5 == 5) --i[w5];
            if (w4 == 4) --i[w4];
            if (w3 == 3) --i[w3];
            if (w2 == 2) --i[w2];
            if (w1 == 1) --i[w1];
            if (w0 == 0) --i[w0];
        }

        if (w7 == 7) HuffmanEncFlush(&huf[w7], &writer);
        if (w6 == 6) HuffmanEncFlush(&huf[w6], &writer);
        if (w5 == 5) HuffmanEncFlush(&huf[w5], &writer);
        if (w4 == 4) HuffmanEncFlush(&huf[w4], &writer);
        if (w3 == 3) HuffmanEncFlush(&huf[w3], &writer);
        if (w2 == 2) HuffmanEncFlush(&huf[w2], &writer);
        if (w1 == 1) HuffmanEncFlush(&huf[w1], &writer);
        if (w0 == 0) HuffmanEncFlush(&huf[w0], &writer);

    }

    return writer.finish(&ctx_buffer);

#undef w7
#undef w6
#undef w5
#undef w4
#undef w3
#undef w2
#undef w1
#undef w0
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

    memset(&ari_, 0, sizeof ari_);
    memset(&cnt_, 255, sizeof cnt_);

    valvec<byte_t> table_huf;
    if (*cp == 255) {
        size_t read = 0;
        decoder d(table.substr(1), &read);
        d.decode(table.substr(1 + read), &table_huf, GetTlsTerarkContext());
        if (psize != nullptr) {
            *psize = table.size();
        }
        cp = table_huf.data();
        end = cp + table_huf.size();
    }
    if (*cp == 0) {
        if (psize != nullptr) {
            *psize = 1;
        }
        return;
    }
    if (*cp != 1) {
        // unknow version
        abort();
    }
    ++cp;

    if (*cp++ != 0) {
        rle_i = 0;
        i = *cp++;
        do {
            HuffmanBuildDTable(&cp, ari_[i], cnt_[i]);

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
    HuffmanBuildDTable(&cp, ari_[256], cnt_[256]);

    if (psize != nullptr && end == table.udata() + table.size()) {
        *psize = cp - table.udata();
    }
}


bool decoder_o1::decode_x1(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    auto bits = EntropyBytesToBits(data);
    return bitwise_decode_x1(bits, record, context);
}

bool decoder_o1::decode_x2(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    auto bits = EntropyBytesToBits(data);
    return bitwise_decode_x2(bits, record, context);
}

bool decoder_o1::decode_x4(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    auto bits = EntropyBytesToBits(data);
    return bitwise_decode_x4(bits, record, context);
}

bool decoder_o1::decode_x8(fstring data, valvec<byte_t>* record, TerarkContext* context) const {
    auto bits = EntropyBytesToBits(data);
    return bitwise_decode_x8(bits, record, context);
}

bool decoder_o1::bitwise_decode_x1(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const {
    record->risk_set_size(0);

    EntropyBitsReader reader(data);
    size_t l = 256;
    HuffmanState huf;
    if (terark_likely(reader.size() > 0)) {
        huf.bit_count = 0;
        huf.bits = 0;
        reader.read((reader.size() - 1) % HEADER_BLOCK_BITS + 1, &huf.bits, &huf.bit_count);
        record->ensure_capacity(HEADER_BLOCK_BITS);
        while (true) {
            if (huf.bit_count < BLOCK_BITS) {
                if (terark_likely(reader.size() > 0)) {
                    reader.read(HEADER_BLOCK_BITS, &huf.bits, &huf.bit_count);
                    record->ensure_capacity(record->size() + HEADER_BLOCK_BITS);
                }
                else if (huf.bit_count == 0) {
                    break;
                }
            }
            byte_t c = ari_[l][huf.bits >> (64 - BLOCK_BITS)];
            uint8_t b = cnt_[l][c];
            if (terark_unlikely(b > huf.bit_count)) return false;
            record->unchecked_push(l = c);
            huf.bits <<= b;
            huf.bit_count -= b;
        }
    }
    return true;
}

bool decoder_o1::bitwise_decode_x2(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const {
    return bitwise_decode_xN<2>(data, record, context);
}

#ifdef __AVX2__

bool decoder_o1::bitwise_decode_x4(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const {
    constexpr size_t N = 4;
    record->risk_set_size(0);

    valvec<byte_t>* output;
    EntropyBitsReader reader(data);
    auto ctx_output = context->alloc();
    output = &ctx_output.get();
    output->risk_set_size(0);
    output->ensure_capacity(N);

    alignas(16) uint32_t l[N] = { 256, 256, 256, 256 };

    struct LocalBuffer {
        byte_t buffer[(BLOCK_BITS * N + 7) / 8];

        byte_t* data() { return buffer; }
        size_t capacity() { return sizeof buffer; }
        void ensure_capacity(size_t) { assert(false); }
    } buffer;

    size_t remain = N - 1;
    if (reader.size() > BLOCK_BITS * N) {
        alignas(16) uint32_t bits[N];

        bits[0] = HuffmanDecStateInit(&reader);
        bits[1] = HuffmanDecStateInit(&reader);
        bits[2] = HuffmanDecStateInit(&reader);
        bits[3] = HuffmanDecStateInit(&reader);

        const __m128i u32_1 = _mm_set1_epi32(1);
        const __m128i u32_7 = _mm_set1_epi32(7);
        const __m128i u32_32 = _mm_set1_epi32(32);
        const __m128i u32_max = _mm_set1_epi32(0xffffffff);
        const __m128i u32_mask = _mm_set1_epi32((1u << BLOCK_BITS) - 1);

        union {
            alignas(16) byte_t c[16];
            alignas(16) uint32_t c_u32;
        };
        while (true) {
            alignas(16) uint32_t s[N], b[N];

            c[0] = ari_[l[0]][bits[0]];
            s[0] = 0 + (b[0] = cnt_[l[0]][c[0]]);

            c[1] = ari_[l[1]][bits[1]];
            s[1] = s[0] + (b[1] = cnt_[l[1]][c[1]]);

            c[2] = ari_[l[2]][bits[2]];
            s[2] = s[1] + (b[2] = cnt_[l[2]][c[2]]);

            c[3] = ari_[l[3]][bits[3]];
            s[3] = s[2] + (b[3] = cnt_[l[3]][c[3]]);

            if (terark_unlikely(s[N - 1] >= reader.size())) {
                break;
            }

            byte_t* output_data = output->data() + output->size();

            (uint32_t&)*output_data = c_u32;
            _mm_store_si128((__m128i*)l, _mm_cvtepu8_epi32(_mm_load_si128((__m128i*)c)));

            // |                                         | <- data
            // |-----------------------------------------|
            // |           byte aligned ->| |<- remain ->|
            // |                   slag ->| |<-         s         ->|
            // |                                |<- shift ->|<- b ->|<- d ->|<- byte aligned
            // |              ptr_start ->|     |<-        uint32         ->|
            // |                          |     |
            // |                         ptr_offset

            intptr_t ptr_start = intptr_t(reader.data_) - 4 - ceiled_div(reader.remain_, 8);
            intptr_t slag = -intptr_t(reader.remain_) & 7;

            __m128i fs = _mm_add_epi32(_mm_load_si128((__m128i*)s), _mm_set1_epi32(slag));
            __m128i d = _mm_andnot_si128(_mm_sub_epi32(fs, u32_1), u32_7);
            __m128i ptr_offset = _mm_srli_epi32(_mm_add_epi32(fs, d), 3);
            __m128i raw_u32 = _mm_i32gather_epi32((int*)ptr_start, ptr_offset, 1);
            __m128i shift = _mm_sub_epi32(u32_32, _mm_add_epi32(_mm_load_si128((__m128i*)b), d));
            __m128i nm = _mm_sllv_epi32(u32_max, _mm_load_si128((__m128i*)b));
            __m128i read = _mm_andnot_si128(nm, _mm_srlv_epi32(raw_u32, shift));
            __m128i slb = _mm_sllv_epi32(_mm_load_si128((__m128i*)bits), _mm_load_si128((__m128i*)b));
            _mm_store_si128((__m128i*)bits, _mm_or_si128(_mm_and_si128(slb, u32_mask), read));

            output->risk_set_size(output->size() + N);
            reader.skip(s[N - 1]);
            output->ensure_capacity(output->size() + N);
        }
        size_t bit_count;

        do {
            byte_t b;
            b = cnt_[l[0]][c[0]]; output->unchecked_push(l[0] = c[0]); if (HuffmanDecBreak(&bits[0], &bit_count, b, &reader)) { remain = 0; break; }
            b = cnt_[l[1]][c[1]]; output->unchecked_push(l[1] = c[1]); if (HuffmanDecBreak(&bits[1], &bit_count, b, &reader)) { remain = 1; break; }
            b = cnt_[l[2]][c[2]]; output->unchecked_push(l[2] = c[2]); if (HuffmanDecBreak(&bits[2], &bit_count, b, &reader)) { remain = 2; break; }
            b = cnt_[l[3]][c[3]]; output->unchecked_push(l[3] = c[3]); if (HuffmanDecBreak(&bits[3], &bit_count, b, &reader)) { remain = 3; break; }

            assert(false);
        } while (false);

        EntropyBitsReverseWriter<LocalBuffer> writer(&buffer);

        if (remain != 0) writer.write(uint64_t(bits[0]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (remain != 1) writer.write(uint64_t(bits[1]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (remain != 2) writer.write(uint64_t(bits[2]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (remain != 3) writer.write(uint64_t(bits[3]) << (64 - BLOCK_BITS), BLOCK_BITS);

        if (remain == 0) writer.write(uint64_t(bits[0]) << (64 - BLOCK_BITS), bit_count);
        if (remain == 1) writer.write(uint64_t(bits[1]) << (64 - BLOCK_BITS), bit_count);
        if (remain == 2) writer.write(uint64_t(bits[2]) << (64 - BLOCK_BITS), bit_count);
        if (remain == 3) writer.write(uint64_t(bits[3]) << (64 - BLOCK_BITS), bit_count);

        reader = writer.finish(nullptr);
        output->ensure_capacity(output->size() + N);
    }

#define CASE(I, w)                                                              \
    case I: {                                                                   \
            if (huf.bit_count < BLOCK_BITS) {                                   \
                if (reader.size() > 0) {                                        \
                    reader.read(HEADER_BLOCK_BITS, &huf.bits, &huf.bit_count);  \
                } else if (huf.bit_count == 0) {                                \
                    break;                                                      \
                }                                                               \
            }                                                                   \
            byte_t c = ari_[l[w]][huf.bits >> (64 - BLOCK_BITS)];               \
            uint8_t b = cnt_[l[w]][c];                                          \
            if (terark_unlikely(b > huf.bit_count)) return false;               \
            output->unchecked_push(l[w] = c);                                   \
            huf.bits <<= b;                                                     \
            huf.bit_count -= b;                                                 \
        }                                                                       \
        // fall through

    HuffmanState huf;
    if (terark_likely(reader.size() > 0)) {
        huf.bit_count = 0;
        huf.bits = 0;
        reader.read((reader.size() - 1) % HEADER_BLOCK_BITS + 1, &huf.bits, &huf.bit_count);
        switch ((remain + 1) % N) {
            while (true) {
                output->ensure_capacity(output->size() + N);
                CASE(0, 0);
                CASE(1, 1);
                CASE(2, 2);
                CASE(3, 3);
            }
        }
    }
#undef CASE

    if (N > 1) {
        size_t size = output->size();
        record->resize_no_init(size);
        byte_t* from = output->data();
        byte_t* to = record->data();
        intptr_t i[N];

        i[0] = 0;
        i[1] = (intptr_t)size / N + (0 < size % N) + i[0];
        i[2] = (intptr_t)size / N + (1 < size % N) + i[1];
        i[3] = (intptr_t)size / N + (2 < size % N) + i[2];

        size_t pos = 0;
        for (size_t end = size - size % N; pos < end; ) {
            to[i[0]++] = from[pos++];
            to[i[1]++] = from[pos++];
            to[i[2]++] = from[pos++];
            to[i[3]++] = from[pos++];
        };
        if (pos < size) to[i[0]++] = from[pos++];
        if (pos < size) to[i[1]++] = from[pos++];
        if (pos < size) to[i[2]++] = from[pos++];
        if (pos < size) to[i[3]++] = from[pos++];
    }

    return true;
}

bool decoder_o1::bitwise_decode_x8(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const {
    constexpr size_t N = 8;
    record->risk_set_size(0);

    valvec<byte_t>* output;
    EntropyBitsReader reader(data);
    auto ctx_output = context->alloc();
    output = &ctx_output.get();
    output->risk_set_size(0);
    output->ensure_capacity(N);

    alignas(32) uint32_t l[N] = { 256, 256, 256, 256, 256, 256, 256, 256 };

    struct LocalBuffer {
        byte_t buffer[(BLOCK_BITS * N + 7) / 8];

        byte_t* data() { return buffer; }
        size_t capacity() { return sizeof buffer; }
        void ensure_capacity(size_t) { assert(false); }
    } buffer;

    size_t remain = N - 1;
    if (reader.size() > BLOCK_BITS * N) {
        alignas(32) uint32_t bits[N];

        bits[0] = HuffmanDecStateInit(&reader);
        bits[1] = HuffmanDecStateInit(&reader);
        bits[2] = HuffmanDecStateInit(&reader);
        bits[3] = HuffmanDecStateInit(&reader);
        bits[4] = HuffmanDecStateInit(&reader);
        bits[5] = HuffmanDecStateInit(&reader);
        bits[6] = HuffmanDecStateInit(&reader);
        bits[7] = HuffmanDecStateInit(&reader);

        const __m256i u32_1 = _mm256_set1_epi32(1);
        const __m256i u32_7 = _mm256_set1_epi32(7);
        const __m256i u32_32 = _mm256_set1_epi32(32);
        const __m256i u32_max = _mm256_set1_epi32(0xffffffff);
        const __m256i u32_mask = _mm256_set1_epi32((1u << BLOCK_BITS) - 1);

        union {
            alignas(16) byte_t c[16];
            alignas(16) uint64_t c_u64;
        };
        while (true) {
            alignas(32) uint32_t s[N], b[N];

            c[0] = ari_[l[0]][bits[0]];
            s[0] =    0 + (b[0] = cnt_[l[0]][c[0]]);

            c[1] = ari_[l[1]][bits[1]];
            s[1] = s[0] + (b[1] = cnt_[l[1]][c[1]]);

            c[2] = ari_[l[2]][bits[2]];
            s[2] = s[1] + (b[2] = cnt_[l[2]][c[2]]);

            c[3] = ari_[l[3]][bits[3]];
            s[3] = s[2] + (b[3] = cnt_[l[3]][c[3]]);

            c[4] = ari_[l[4]][bits[4]];
            s[4] = s[3] + (b[4] = cnt_[l[4]][c[4]]);

            c[5] = ari_[l[5]][bits[5]];
            s[5] = s[4] + (b[5] = cnt_[l[5]][c[5]]);

            c[6] = ari_[l[6]][bits[6]];
            s[6] = s[5] + (b[6] = cnt_[l[6]][c[6]]);

            c[7] = ari_[l[7]][bits[7]];
            s[7] = s[6] + (b[7] = cnt_[l[7]][c[7]]);

            if (terark_unlikely(s[N - 1] >= reader.size())) {
                break;
            }

            byte_t* output_data = output->data() + output->size();

            (uint64_t&)*output_data = c_u64;
            _mm256_store_si256((__m256i*)l, _mm256_cvtepu8_epi32(_mm_load_si128((__m128i*)c)));

            // |                                         | <- data
            // |-----------------------------------------|
            // |           byte aligned ->| |<- remain ->|
            // |                   slag ->| |<-         s         ->|
            // |                                |<- shift ->|<- b ->|<- d ->|<- byte aligned
            // |              ptr_start ->|     |<-        uint32         ->|
            // |                          |     |
            // |                         ptr_offset

            intptr_t ptr_start = intptr_t(reader.data_) - 4 - ceiled_div(reader.remain_, 8);
            intptr_t slag = -intptr_t(reader.remain_) & 7;

            __m256i fs = _mm256_add_epi32(_mm256_load_si256((__m256i*)s), _mm256_set1_epi32(slag));
            __m256i d = _mm256_andnot_si256(_mm256_sub_epi32(fs, u32_1), u32_7);
            __m256i ptr_offset = _mm256_srli_epi32(_mm256_add_epi32(fs, d), 3);
            __m256i raw_u32 = _mm256_i32gather_epi32((int*)ptr_start, ptr_offset, 1);
            __m256i shift = _mm256_sub_epi32(u32_32, _mm256_add_epi32(_mm256_load_si256((__m256i*)b), d));
            __m256i nm = _mm256_sllv_epi32(u32_max, _mm256_load_si256((__m256i*)b));
            __m256i read = _mm256_andnot_si256(nm, _mm256_srlv_epi32(raw_u32, shift));
            __m256i slb = _mm256_sllv_epi32(_mm256_load_si256((__m256i*)bits), _mm256_load_si256((__m256i*)b));
            _mm256_store_si256((__m256i*)bits, _mm256_or_si256(_mm256_and_si256(slb, u32_mask), read));

            output->risk_set_size(output->size() + N);
            reader.skip(s[N - 1]);
            output->ensure_capacity(output->size() + N);
        }
        size_t bit_count;

        do {
            byte_t b;
            b = cnt_[l[0]][c[0]]; output->unchecked_push(l[0] = c[0]); if (HuffmanDecBreak(&bits[0], &bit_count, b, &reader)) { remain = 0; break; }
            b = cnt_[l[1]][c[1]]; output->unchecked_push(l[1] = c[1]); if (HuffmanDecBreak(&bits[1], &bit_count, b, &reader)) { remain = 1; break; }
            b = cnt_[l[2]][c[2]]; output->unchecked_push(l[2] = c[2]); if (HuffmanDecBreak(&bits[2], &bit_count, b, &reader)) { remain = 2; break; }
            b = cnt_[l[3]][c[3]]; output->unchecked_push(l[3] = c[3]); if (HuffmanDecBreak(&bits[3], &bit_count, b, &reader)) { remain = 3; break; }
            b = cnt_[l[4]][c[4]]; output->unchecked_push(l[4] = c[4]); if (HuffmanDecBreak(&bits[4], &bit_count, b, &reader)) { remain = 4; break; }
            b = cnt_[l[5]][c[5]]; output->unchecked_push(l[5] = c[5]); if (HuffmanDecBreak(&bits[5], &bit_count, b, &reader)) { remain = 5; break; }
            b = cnt_[l[6]][c[6]]; output->unchecked_push(l[6] = c[6]); if (HuffmanDecBreak(&bits[6], &bit_count, b, &reader)) { remain = 6; break; }
            b = cnt_[l[7]][c[7]]; output->unchecked_push(l[7] = c[7]); if (HuffmanDecBreak(&bits[7], &bit_count, b, &reader)) { remain = 7; break; }

            assert(false);
        } while (false);

        EntropyBitsReverseWriter<LocalBuffer> writer(&buffer);

        if (remain != 0) writer.write(uint64_t(bits[0]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (remain != 1) writer.write(uint64_t(bits[1]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (remain != 2) writer.write(uint64_t(bits[2]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (remain != 3) writer.write(uint64_t(bits[3]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (remain != 4) writer.write(uint64_t(bits[4]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (remain != 5) writer.write(uint64_t(bits[5]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (remain != 6) writer.write(uint64_t(bits[6]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (remain != 7) writer.write(uint64_t(bits[7]) << (64 - BLOCK_BITS), BLOCK_BITS);

        if (remain == 0) writer.write(uint64_t(bits[0]) << (64 - BLOCK_BITS), bit_count);
        if (remain == 1) writer.write(uint64_t(bits[1]) << (64 - BLOCK_BITS), bit_count);
        if (remain == 2) writer.write(uint64_t(bits[2]) << (64 - BLOCK_BITS), bit_count);
        if (remain == 3) writer.write(uint64_t(bits[3]) << (64 - BLOCK_BITS), bit_count);
        if (remain == 4) writer.write(uint64_t(bits[4]) << (64 - BLOCK_BITS), bit_count);
        if (remain == 5) writer.write(uint64_t(bits[5]) << (64 - BLOCK_BITS), bit_count);
        if (remain == 6) writer.write(uint64_t(bits[6]) << (64 - BLOCK_BITS), bit_count);
        if (remain == 7) writer.write(uint64_t(bits[7]) << (64 - BLOCK_BITS), bit_count);

        reader = writer.finish(nullptr);
        output->ensure_capacity(output->size() + N);
    }

#define CASE(I, w)                                                              \
    case I: {                                                                   \
            if (huf.bit_count < BLOCK_BITS) {                                   \
                if (reader.size() > 0) {                                        \
                    reader.read(HEADER_BLOCK_BITS, &huf.bits, &huf.bit_count);  \
                } else if (huf.bit_count == 0) {                                \
                    break;                                                      \
                }                                                               \
            }                                                                   \
            byte_t c = ari_[l[w]][huf.bits >> (64 - BLOCK_BITS)];               \
            uint8_t b = cnt_[l[w]][c];                                          \
            if (terark_unlikely(b > huf.bit_count)) return false;               \
            output->unchecked_push(l[w] = c);                                   \
            huf.bits <<= b;                                                     \
            huf.bit_count -= b;                                                 \
        }                                                                       \
        // fall through

    HuffmanState huf;
    if (terark_likely(reader.size() > 0)) {
        huf.bit_count = 0;
        huf.bits = 0;
        reader.read((reader.size() - 1) % HEADER_BLOCK_BITS + 1, &huf.bits, &huf.bit_count);
        switch ((remain + 1) % N) {
            while (true) {
                output->ensure_capacity(output->size() + N);
                CASE(0, 0);
                CASE(1, 1);
                CASE(2, 2);
                CASE(3, 3);
                CASE(4, 4);
                CASE(5, 5);
                CASE(6, 6);
                CASE(7, 7);
            }
        }
    }
#undef CASE

    if (N > 1) {
        size_t size = output->size();
        record->resize_no_init(size);
        byte_t* from = output->data();
        byte_t* to = record->data();
        intptr_t i[N];

        i[0] = 0;
        i[1] = (intptr_t)size / N + (0 < size % N) + i[0];
        i[2] = (intptr_t)size / N + (1 < size % N) + i[1];
        i[3] = (intptr_t)size / N + (2 < size % N) + i[2];
        i[4] = (intptr_t)size / N + (3 < size % N) + i[3];
        i[5] = (intptr_t)size / N + (4 < size % N) + i[4];
        i[6] = (intptr_t)size / N + (5 < size % N) + i[5];
        i[7] = (intptr_t)size / N + (6 < size % N) + i[6];

        size_t pos = 0;
        for (size_t end = size - size % N; pos < end; ) {
            to[i[0]++] = from[pos++];
            to[i[1]++] = from[pos++];
            to[i[2]++] = from[pos++];
            to[i[3]++] = from[pos++];
            to[i[4]++] = from[pos++];
            to[i[5]++] = from[pos++];
            to[i[6]++] = from[pos++];
            to[i[7]++] = from[pos++];
        };
        if (pos < size) to[i[0]++] = from[pos++];
        if (pos < size) to[i[1]++] = from[pos++];
        if (pos < size) to[i[2]++] = from[pos++];
        if (pos < size) to[i[3]++] = from[pos++];
        if (pos < size) to[i[4]++] = from[pos++];
        if (pos < size) to[i[5]++] = from[pos++];
        if (pos < size) to[i[6]++] = from[pos++];
        if (pos < size) to[i[7]++] = from[pos++];
    }

    return true;
}

#else

bool decoder_o1::bitwise_decode_x4(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const {
    return bitwise_decode_xN<4>(data, record, context);
}

bool decoder_o1::bitwise_decode_x8(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const {
    return bitwise_decode_xN<8>(data, record, context);
}

#endif

template<size_t N>
bool decoder_o1::bitwise_decode_xN(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const {
    record->risk_set_size(0);

#define w0 0
#define w1 (N >= 2 ? 1 : 0)
#define w2 (N >= 4 ? 2 : 0)
#define w3 (N >= 4 ? 3 : 0)
#define w4 (N >= 8 ? 4 : 0)
#define w5 (N >= 8 ? 5 : 0)
#define w6 (N >= 8 ? 6 : 0)
#define w7 (N >= 8 ? 7 : 0)

    valvec<byte_t>* output;
    EntropyBitsReader reader(data);
    ContextBuffer ctx_output;
    if (N == 1) {
        output = record;
    }
    else {
        ctx_output = context->alloc();
        output = &ctx_output.get();
        output->risk_set_size(0);
    }
    output->ensure_capacity(N);

    size_t l[N];

    if (w0 == 0) l[w0] = 256;
    if (w1 == 1) l[w1] = 256;
    if (w2 == 2) l[w2] = 256;
    if (w3 == 3) l[w3] = 256;
    if (w4 == 4) l[w4] = 256;
    if (w5 == 5) l[w5] = 256;
    if (w6 == 6) l[w6] = 256;
    if (w7 == 7) l[w7] = 256;

    struct LocalBuffer {
        byte_t buffer[(BLOCK_BITS * N + 7) / 8];

        byte_t* data() { return buffer; }
        size_t capacity() { return sizeof buffer; }
        void ensure_capacity(size_t) { assert(false); }
    } buffer;

    size_t remain = N - 1;
    if (reader.size() > BLOCK_BITS * N) {
        uint16_t bits[N];

        if (w0 == 0) bits[w0] = HuffmanDecStateInit(&reader);
        if (w1 == 1) bits[w1] = HuffmanDecStateInit(&reader);
        if (w2 == 2) bits[w2] = HuffmanDecStateInit(&reader);
        if (w3 == 3) bits[w3] = HuffmanDecStateInit(&reader);
        if (w4 == 4) bits[w4] = HuffmanDecStateInit(&reader);
        if (w5 == 5) bits[w5] = HuffmanDecStateInit(&reader);
        if (w6 == 6) bits[w6] = HuffmanDecStateInit(&reader);
        if (w7 == 7) bits[w7] = HuffmanDecStateInit(&reader);

        byte_t c[N];
        while (true) {
            size_t s[N];
            uint8_t b[N];

            if (w0 == 0) c[w0] = ari_[l[w0]][bits[w0]];
            if (w0 == 0) s[w0] =     0 + (b[w0] = cnt_[l[w0]][c[w0]]);

            if (w1 == 1) c[w1] = ari_[l[w1]][bits[w1]];
            if (w1 == 1) s[w1] = s[w0] + (b[w1] = cnt_[l[w1]][c[w1]]);

            if (w2 == 2) c[w2] = ari_[l[w2]][bits[w2]];
            if (w2 == 2) s[w2] = s[w1] + (b[w2] = cnt_[l[w2]][c[w2]]);

            if (w3 == 3) c[w3] = ari_[l[w3]][bits[w3]];
            if (w3 == 3) s[w3] = s[w2] + (b[w3] = cnt_[l[w3]][c[w3]]);

            if (w4 == 4) c[w4] = ari_[l[w4]][bits[w4]];
            if (w4 == 4) s[w4] = s[w3] + (b[w4] = cnt_[l[w4]][c[w4]]);

            if (w5 == 5) c[w5] = ari_[l[w5]][bits[w5]];
            if (w5 == 5) s[w5] = s[w4] + (b[w5] = cnt_[l[w5]][c[w5]]);

            if (w6 == 6) c[w6] = ari_[l[w6]][bits[w6]];
            if (w6 == 6) s[w6] = s[w5] + (b[w6] = cnt_[l[w6]][c[w6]]);

            if (w7 == 7) c[w7] = ari_[l[w7]][bits[w7]];
            if (w7 == 7) s[w7] = s[w6] + (b[w7] = cnt_[l[w7]][c[w7]]);

            if (terark_unlikely(s[N - 1] >= reader.size())) {
                break;
            }

            byte_t* output_data = output->data() + output->size();

            if (w0 == 0) output_data[w0] = (l[w0] = c[w0]);
            if (w1 == 1) output_data[w1] = (l[w1] = c[w1]);
            if (w2 == 2) output_data[w2] = (l[w2] = c[w2]);
            if (w3 == 3) output_data[w3] = (l[w3] = c[w3]);
            if (w4 == 4) output_data[w4] = (l[w4] = c[w4]);
            if (w5 == 5) output_data[w5] = (l[w5] = c[w5]);
            if (w6 == 6) output_data[w6] = (l[w6] = c[w6]);
            if (w7 == 7) output_data[w7] = (l[w7] = c[w7]);

            // |                                         | <- data
            // |-----------------------------------------|
            // |                            |<- remain ->|
            // |                            |<-        end        ->|
            // |                                    |<- bit_count ->|<- d ->|
            // |                                 |<-      uint32 ptr      ->|
            uintptr_t bit_start = (uintptr_t)reader.data_ * 8 - reader.remain_ - 32;
            uintptr_t ptr_value[N];

            if (w0 == 0) ptr_value[w0] = bit_start + s[w0];
            if (w1 == 1) ptr_value[w1] = bit_start + s[w1];
            if (w2 == 2) ptr_value[w2] = bit_start + s[w2];
            if (w3 == 3) ptr_value[w3] = bit_start + s[w3];
            if (w4 == 4) ptr_value[w4] = bit_start + s[w4];
            if (w5 == 5) ptr_value[w5] = bit_start + s[w5];
            if (w6 == 6) ptr_value[w6] = bit_start + s[w6];
            if (w7 == 7) ptr_value[w7] = bit_start + s[w7];

            size_t d[N];

            if (w0 == 0) d[w0] = (~ptr_value[w0] + 1) % 8;
            if (w1 == 1) d[w1] = (~ptr_value[w1] + 1) % 8;
            if (w2 == 2) d[w2] = (~ptr_value[w2] + 1) % 8;
            if (w3 == 3) d[w3] = (~ptr_value[w3] + 1) % 8;
            if (w4 == 4) d[w4] = (~ptr_value[w4] + 1) % 8;
            if (w5 == 5) d[w5] = (~ptr_value[w5] + 1) % 8;
            if (w6 == 6) d[w6] = (~ptr_value[w6] + 1) % 8;
            if (w7 == 7) d[w7] = (~ptr_value[w7] + 1) % 8;
                
            uint32_t u32value[N];

            if (w0 == 0) u32value[w0] = *(uint32_t*)((ptr_value[w0] + d[w0]) / 8);
            if (w1 == 1) u32value[w1] = *(uint32_t*)((ptr_value[w1] + d[w1]) / 8);
            if (w2 == 2) u32value[w2] = *(uint32_t*)((ptr_value[w2] + d[w2]) / 8);
            if (w3 == 3) u32value[w3] = *(uint32_t*)((ptr_value[w3] + d[w3]) / 8);
            if (w4 == 4) u32value[w4] = *(uint32_t*)((ptr_value[w4] + d[w4]) / 8);
            if (w5 == 5) u32value[w5] = *(uint32_t*)((ptr_value[w5] + d[w5]) / 8);
            if (w6 == 6) u32value[w6] = *(uint32_t*)((ptr_value[w6] + d[w6]) / 8);
            if (w7 == 7) u32value[w7] = *(uint32_t*)((ptr_value[w7] + d[w7]) / 8);
                
            uint16_t read_bits[N];
#ifdef __BMI2__
            if (w0 == 0) read_bits[w0] = uint16_t(_bextr_u32(u32value[w0], 32 - d[w0] - b[w0], b[w0]));
            if (w1 == 1) read_bits[w1] = uint16_t(_bextr_u32(u32value[w1], 32 - d[w1] - b[w1], b[w1]));
            if (w2 == 2) read_bits[w2] = uint16_t(_bextr_u32(u32value[w2], 32 - d[w2] - b[w2], b[w2]));
            if (w3 == 3) read_bits[w3] = uint16_t(_bextr_u32(u32value[w3], 32 - d[w3] - b[w3], b[w3]));
            if (w4 == 4) read_bits[w4] = uint16_t(_bextr_u32(u32value[w4], 32 - d[w4] - b[w4], b[w4]));
            if (w5 == 5) read_bits[w5] = uint16_t(_bextr_u32(u32value[w5], 32 - d[w5] - b[w5], b[w5]));
            if (w6 == 6) read_bits[w6] = uint16_t(_bextr_u32(u32value[w6], 32 - d[w6] - b[w6], b[w6]));
            if (w7 == 7) read_bits[w7] = uint16_t(_bextr_u32(u32value[w7], 32 - d[w7] - b[w7], b[w7]));
#else
            static constexpr uint32_t mask[] = {
#   define MAKE_MASK(z, n, u) ~(0xffffffffu << n),
                BOOST_PP_REPEAT(32, MAKE_MASK, ~)
#   undef MAKE_MASK
            };
            if (w0 == 0) read_bits[w0] = uint16_t((u32value[w0] >> (32 - d[w0] - b[w0])) & mask[b[w0]]);
            if (w1 == 1) read_bits[w1] = uint16_t((u32value[w1] >> (32 - d[w1] - b[w1])) & mask[b[w1]]);
            if (w2 == 2) read_bits[w2] = uint16_t((u32value[w2] >> (32 - d[w2] - b[w2])) & mask[b[w2]]);
            if (w3 == 3) read_bits[w3] = uint16_t((u32value[w3] >> (32 - d[w3] - b[w3])) & mask[b[w3]]);
            if (w4 == 4) read_bits[w4] = uint16_t((u32value[w4] >> (32 - d[w4] - b[w4])) & mask[b[w4]]);
            if (w5 == 5) read_bits[w5] = uint16_t((u32value[w5] >> (32 - d[w5] - b[w5])) & mask[b[w5]]);
            if (w6 == 6) read_bits[w6] = uint16_t((u32value[w6] >> (32 - d[w6] - b[w6])) & mask[b[w6]]);
            if (w7 == 7) read_bits[w7] = uint16_t((u32value[w7] >> (32 - d[w7] - b[w7])) & mask[b[w7]]);
#endif
            if (w0 == 0) bits[w0] = ((bits[w0] << b[w0]) & uint16_t((1u << BLOCK_BITS) - 1)) | read_bits[w0];
            if (w1 == 1) bits[w1] = ((bits[w1] << b[w1]) & uint16_t((1u << BLOCK_BITS) - 1)) | read_bits[w1];
            if (w2 == 2) bits[w2] = ((bits[w2] << b[w2]) & uint16_t((1u << BLOCK_BITS) - 1)) | read_bits[w2];
            if (w3 == 3) bits[w3] = ((bits[w3] << b[w3]) & uint16_t((1u << BLOCK_BITS) - 1)) | read_bits[w3];
            if (w4 == 4) bits[w4] = ((bits[w4] << b[w4]) & uint16_t((1u << BLOCK_BITS) - 1)) | read_bits[w4];
            if (w5 == 5) bits[w5] = ((bits[w5] << b[w5]) & uint16_t((1u << BLOCK_BITS) - 1)) | read_bits[w5];
            if (w6 == 6) bits[w6] = ((bits[w6] << b[w6]) & uint16_t((1u << BLOCK_BITS) - 1)) | read_bits[w6];
            if (w7 == 7) bits[w7] = ((bits[w7] << b[w7]) & uint16_t((1u << BLOCK_BITS) - 1)) | read_bits[w7];

            output->risk_set_size(output->size() + N);
            reader.skip(s[N - 1]);
            output->ensure_capacity(output->size() + N);
        }
        size_t bit_count;

        do {
            if (w0 == 0) { byte_t b = cnt_[l[w0]][c[w0]]; output->unchecked_push(l[w0] = c[w0]); if (HuffmanDecBreak(&bits[w0], &bit_count, b, &reader)) { remain = 0; break; } }
            if (w1 == 1) { byte_t b = cnt_[l[w1]][c[w1]]; output->unchecked_push(l[w1] = c[w1]); if (HuffmanDecBreak(&bits[w1], &bit_count, b, &reader)) { remain = 1; break; } }
            if (w2 == 2) { byte_t b = cnt_[l[w2]][c[w2]]; output->unchecked_push(l[w2] = c[w2]); if (HuffmanDecBreak(&bits[w2], &bit_count, b, &reader)) { remain = 2; break; } }
            if (w3 == 3) { byte_t b = cnt_[l[w3]][c[w3]]; output->unchecked_push(l[w3] = c[w3]); if (HuffmanDecBreak(&bits[w3], &bit_count, b, &reader)) { remain = 3; break; } }
            if (w4 == 4) { byte_t b = cnt_[l[w4]][c[w4]]; output->unchecked_push(l[w4] = c[w4]); if (HuffmanDecBreak(&bits[w4], &bit_count, b, &reader)) { remain = 4; break; } }
            if (w5 == 5) { byte_t b = cnt_[l[w5]][c[w5]]; output->unchecked_push(l[w5] = c[w5]); if (HuffmanDecBreak(&bits[w5], &bit_count, b, &reader)) { remain = 5; break; } }
            if (w6 == 6) { byte_t b = cnt_[l[w6]][c[w6]]; output->unchecked_push(l[w6] = c[w6]); if (HuffmanDecBreak(&bits[w6], &bit_count, b, &reader)) { remain = 6; break; } }
            if (w7 == 7) { byte_t b = cnt_[l[w7]][c[w7]]; output->unchecked_push(l[w7] = c[w7]); if (HuffmanDecBreak(&bits[w7], &bit_count, b, &reader)) { remain = 7; break; } }

            assert(false);
        } while (false);

        EntropyBitsReverseWriter<LocalBuffer> writer(&buffer);

        if (w0 == 0 && remain != 0) writer.write(uint64_t(bits[w0]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (w1 == 1 && remain != 1) writer.write(uint64_t(bits[w1]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (w2 == 2 && remain != 2) writer.write(uint64_t(bits[w2]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (w3 == 3 && remain != 3) writer.write(uint64_t(bits[w3]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (w4 == 4 && remain != 4) writer.write(uint64_t(bits[w4]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (w5 == 5 && remain != 5) writer.write(uint64_t(bits[w5]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (w6 == 6 && remain != 6) writer.write(uint64_t(bits[w6]) << (64 - BLOCK_BITS), BLOCK_BITS);
        if (w7 == 7 && remain != 7) writer.write(uint64_t(bits[w7]) << (64 - BLOCK_BITS), BLOCK_BITS);

        if (w0 == 0 && remain == 0) writer.write(uint64_t(bits[w0]) << (64 - BLOCK_BITS), bit_count);
        if (w1 == 1 && remain == 1) writer.write(uint64_t(bits[w1]) << (64 - BLOCK_BITS), bit_count);
        if (w2 == 2 && remain == 2) writer.write(uint64_t(bits[w2]) << (64 - BLOCK_BITS), bit_count);
        if (w3 == 3 && remain == 3) writer.write(uint64_t(bits[w3]) << (64 - BLOCK_BITS), bit_count);
        if (w4 == 4 && remain == 4) writer.write(uint64_t(bits[w4]) << (64 - BLOCK_BITS), bit_count);
        if (w5 == 5 && remain == 5) writer.write(uint64_t(bits[w5]) << (64 - BLOCK_BITS), bit_count);
        if (w6 == 6 && remain == 6) writer.write(uint64_t(bits[w6]) << (64 - BLOCK_BITS), bit_count);
        if (w7 == 7 && remain == 7) writer.write(uint64_t(bits[w7]) << (64 - BLOCK_BITS), bit_count);

        reader = writer.finish(nullptr);
        output->ensure_capacity(output->size() + N);
    }

#define CASE(I, w)                                                              \
    case I:                                                                     \
        if (w == I) {                                                           \
            if (huf.bit_count < BLOCK_BITS) {                                   \
                if (reader.size() > 0) {                                        \
                    reader.read(HEADER_BLOCK_BITS, &huf.bits, &huf.bit_count);  \
                } else if (huf.bit_count == 0) {                                \
                    break;                                                      \
                }                                                               \
            }                                                                   \
            byte_t c = ari_[l[w]][huf.bits >> (64 - BLOCK_BITS)];               \
            uint8_t b = cnt_[l[w]][c];                                          \
            if (terark_unlikely(b > huf.bit_count)) return false;               \
            output->unchecked_push(l[w] = c);                                   \
            huf.bits <<= b;                                                     \
            huf.bit_count -= b;                                                 \
        }                                                                       \
        // fall through

    HuffmanState huf;
    if (terark_likely(reader.size() > 0)) {
        huf.bit_count = 0;
        huf.bits = 0;
        reader.read((reader.size() - 1) % HEADER_BLOCK_BITS + 1, &huf.bits, &huf.bit_count);
        switch ((remain + 1) % N) {
            while (true) {
                output->ensure_capacity(output->size() + N);
                CASE(0, w0);
                CASE(1, w1);
                CASE(2, w2);
                CASE(3, w3);
                CASE(4, w4);
                CASE(5, w5);
                CASE(6, w6);
                CASE(7, w7);
            }
        }
    }
#undef CASE

    if (N > 1) {
        size_t size = output->size();
        record->resize_no_init(size);
        byte_t* from = output->data();
        byte_t* to = record->data();
        intptr_t i[N];

        if (w0 == 0) i[w0] = 0;
        if (w1 == 1) i[w1] = (intptr_t)size / N + (0 < size % N) + i[w0];
        if (w2 == 2) i[w2] = (intptr_t)size / N + (1 < size % N) + i[w1];
        if (w3 == 3) i[w3] = (intptr_t)size / N + (2 < size % N) + i[w2];
        if (w4 == 4) i[w4] = (intptr_t)size / N + (3 < size % N) + i[w3];
        if (w5 == 5) i[w5] = (intptr_t)size / N + (4 < size % N) + i[w4];
        if (w6 == 6) i[w6] = (intptr_t)size / N + (5 < size % N) + i[w5];
        if (w7 == 7) i[w7] = (intptr_t)size / N + (6 < size % N) + i[w6];

        size_t pos = 0;
        for (size_t end = size - size % N; pos < end; ) {
            if (w0 == 0) to[i[w0]++] = from[pos++];
            if (w1 == 1) to[i[w1]++] = from[pos++];
            if (w2 == 2) to[i[w2]++] = from[pos++];
            if (w3 == 3) to[i[w3]++] = from[pos++];
            if (w4 == 4) to[i[w4]++] = from[pos++];
            if (w5 == 5) to[i[w5]++] = from[pos++];
            if (w6 == 6) to[i[w6]++] = from[pos++];
            if (w7 == 7) to[i[w7]++] = from[pos++];
        };
        if (w0 == 0 && pos < size) to[i[w0]++] = from[pos++];
        if (w1 == 1 && pos < size) to[i[w1]++] = from[pos++];
        if (w2 == 2 && pos < size) to[i[w2]++] = from[pos++];
        if (w3 == 3 && pos < size) to[i[w3]++] = from[pos++];
        if (w4 == 4 && pos < size) to[i[w4]++] = from[pos++];
        if (w5 == 5 && pos < size) to[i[w5]++] = from[pos++];
        if (w6 == 6 && pos < size) to[i[w6]++] = from[pos++];
        if (w7 == 7 && pos < size) to[i[w7]++] = from[pos++];
    }

#undef w0
#undef w1
#undef w2
#undef w3
#undef w4
#undef w5
#undef w6
#undef w7
    return true;
}

// --------------------------------------------------------------------------


}}
