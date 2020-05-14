#pragma once

#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <boost/integer/static_log2.hpp>
#include "entropy_base.hpp"

namespace terark { namespace Huffman {

static constexpr size_t BLOCK_BITS = 12;
static constexpr size_t NORMALISE = 1ull << 15;

struct HuffmanEncSymbol {
    uint16_t bits;
    uint16_t bit_count;
};

class TERARK_DLL_EXPORT encoder {
public:
    encoder();
    encoder(const freq_hist::histogram_t& hist);

    void init(const freq_hist::histogram_t& hist);

    const valvec<byte_t>& table() const;
    void take_table(valvec<byte_t>* ptr);

    EntropyBytes encode(fstring record, TerarkContext* context) const;

    EntropyBits bitwise_encode(fstring record, TerarkContext* context) const;

    HuffmanEncSymbol syms_[256];
    valvec<byte_t> table_;
};

class TERARK_DLL_EXPORT decoder {
public:
    decoder();
    decoder(fstring table, size_t* psize = nullptr);

    void init(fstring table, size_t* psize);

    bool decode(fstring data, valvec<byte_t>* record, TerarkContext* context) const;

    bool bitwise_decode(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const;

private:
    byte_t ari_[1u << BLOCK_BITS];
    uint8_t cnt_[256];
};

class TERARK_DLL_EXPORT encoder_o1 {
public:
    encoder_o1();
    encoder_o1(const freq_hist_o1::histogram_t& hist);

    void init(const freq_hist_o1::histogram_t& hist);

    const valvec<byte_t>& table() const;
    void take_table(valvec<byte_t>* ptr);

    EntropyBytes encode_x1(fstring record, TerarkContext* context) const;
    EntropyBytes encode_x2(fstring record, TerarkContext* context) const;
    EntropyBytes encode_x4(fstring record, TerarkContext* context) const;
    EntropyBytes encode_x8(fstring record, TerarkContext* context) const;

    EntropyBits bitwise_encode_x1(fstring record, TerarkContext* context) const;
    EntropyBits bitwise_encode_x2(fstring record, TerarkContext* context) const;
    EntropyBits bitwise_encode_x4(fstring record, TerarkContext* context) const;
    EntropyBits bitwise_encode_x8(fstring record, TerarkContext* context) const;

private:
    template<size_t N>
    EntropyBits bitwise_encode_xN(fstring record, TerarkContext* context) const;

    HuffmanEncSymbol syms_[257][256];
    valvec<byte_t> table_;
};

class TERARK_DLL_EXPORT decoder_o1 {
public:
    decoder_o1();
    decoder_o1(fstring table, size_t* psize = nullptr);
    decoder_o1(fstring table, bool nfm, size_t* psize = nullptr);

    void init(fstring table, size_t* psize);

    bool decode_x1(fstring data, valvec<byte_t>* record, TerarkContext* context) const;
    bool decode_x2(fstring data, valvec<byte_t>* record, TerarkContext* context) const;
    bool decode_x4(fstring data, valvec<byte_t>* record, TerarkContext* context) const;
    bool decode_x8(fstring data, valvec<byte_t>* record, TerarkContext* context) const;

    bool bitwise_decode_x1(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const;
    bool bitwise_decode_x2(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const;
    bool bitwise_decode_x4(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const;
    bool bitwise_decode_x8(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const;

    bool not_from_mem_ = false;
private:
    template<size_t N>
    bool bitwise_decode_xN(const EntropyBits& data, valvec<byte_t>* record, TerarkContext* context) const;

    byte_t ari_[257][1u << BLOCK_BITS];
    uint8_t cnt_[257][256];
};

}}
