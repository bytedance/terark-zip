#pragma once

#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include "entropy_base.hpp"

namespace terark { namespace rANS_static_64 {

    // 10 and 11 can be quite a bit faster, but 1-2% larger
    static constexpr size_t TF_SHIFT = 12;
    static constexpr size_t TOTFREQ = 1ull << TF_SHIFT;
    static constexpr size_t NORMALISE = TOTFREQ;

    struct Rans64EncSymbol {
        uint64_t rcp_freq;  // Fixed-point reciprocal frequency
        uint16_t freq;      // Symbol frequency
        uint16_t bias;      // Bias
        uint16_t cmpl_freq; // Complement of frequency: (1 << scale_bits) - freq
        uint16_t rcp_shift; // Reciprocal shift
    };

    struct Rans64DecSymbol {
        uint16_t start;     // Start of range.
        uint16_t freq;      // Symbol frequency.
    };

    struct XCheck {
        size_t enable : 1;
        size_t value : 1;
    };

    TERARK_DLL_EXPORT fstring encode(fstring record, EntropyContext* context);
    TERARK_DLL_EXPORT size_t decode(fstring data, valvec<byte_t>* record, EntropyContext* context);

    TERARK_DLL_EXPORT fstring encode_o1(fstring record, EntropyContext* context);
    TERARK_DLL_EXPORT size_t decode_o1(fstring data, valvec<byte_t>* record, EntropyContext* context);

    TERARK_DLL_EXPORT fstring encode_o2(fstring record, EntropyContext* context);
    TERARK_DLL_EXPORT size_t decode_o2(fstring data, valvec<byte_t>* record, EntropyContext* context);

    class TERARK_DLL_EXPORT encoder {
    public:
        encoder();
        encoder(const freq_hist::histogram_t& hist);

        void init(const freq_hist::histogram_t& hist);

        const valvec<byte_t>& table() const;

        fstring encode   (fstring record, EntropyContext* context) const;
        fstring encode_x1(fstring record, EntropyContext* context) const;
        fstring encode_x2(fstring record, EntropyContext* context) const;
        fstring encode_x4(fstring record, EntropyContext* context) const;
        fstring encode_x8(fstring record, EntropyContext* context) const;

    private:
        template<size_t N>
        fstring encode_xN(fstring record, EntropyContext* context, bool check) const;

        Rans64EncSymbol syms_[256];
        valvec<byte_t> table_;
    };

    class TERARK_DLL_EXPORT decoder {
    public:
        decoder();
        decoder(fstring table, size_t* psize = nullptr);

        void init(fstring table, size_t* psize);

        size_t decode   (fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x1(fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x2(fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x4(fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x8(fstring data, valvec<byte_t>* record, EntropyContext* context) const;

    private:
        template<size_t N>
        size_t decode_xN(fstring data, valvec<byte_t>* record, EntropyContext* context, bool check) const;

        Rans64DecSymbol syms_[256];
        byte_t ari_[TOTFREQ];
    };

    class TERARK_DLL_EXPORT encoder_o1 {
    public:
        encoder_o1();
        encoder_o1(const freq_hist_o1::histogram_t& hist);

        void init(const freq_hist_o1::histogram_t& hist);

        const valvec<byte_t>& table() const;

        fstring encode   (fstring record, EntropyContext* context) const;
        fstring encode_x1(fstring record, EntropyContext* context) const;
        fstring encode_x2(fstring record, EntropyContext* context) const;
        fstring encode_x4(fstring record, EntropyContext* context) const;
        fstring encode_x8(fstring record, EntropyContext* context) const;

    private:
        template<size_t N>
        fstring encode_xN(fstring record, EntropyContext* context, bool check) const;

        Rans64EncSymbol syms_[257][256];
        valvec<byte_t> table_;
    };

    class TERARK_DLL_EXPORT decoder_o1 {
    public:
        decoder_o1();
        decoder_o1(fstring table, size_t* psize = nullptr);

        void init(fstring table, size_t* psize);

        size_t decode   (fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x1(fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x2(fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x4(fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x8(fstring data, valvec<byte_t>* record, EntropyContext* context) const;

    private:
        template<size_t N>
        size_t decode_xN(fstring data, valvec<byte_t>* record, EntropyContext* context, bool check) const;

        Rans64DecSymbol syms_[257][256];
        byte_t ari_[257][TOTFREQ];
    };


    class TERARK_DLL_EXPORT encoder_o2 {
    public:
        encoder_o2();
        encoder_o2(const freq_hist_o2::histogram_t& hist);

        void init(const freq_hist_o2::histogram_t& hist);

        const valvec<byte_t>& table() const;

        fstring encode(fstring record, EntropyContext* context) const;
        fstring encode_x1(fstring record, EntropyContext* context) const;
        fstring encode_x2(fstring record, EntropyContext* context) const;
        fstring encode_x4(fstring record, EntropyContext* context) const;
        fstring encode_x8(fstring record, EntropyContext* context) const;

    private:
        template<size_t N>
        fstring encode_xN(fstring record, EntropyContext* context, bool check) const;

        Rans64EncSymbol syms_[257][257][256];
        valvec<byte_t> table_;
    };

    class TERARK_DLL_EXPORT decoder_o2 {
    public:
        decoder_o2();
        decoder_o2(fstring table, size_t* psize = nullptr);

        void init(fstring table, size_t* psize);

        size_t decode(fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x1(fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x2(fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x4(fstring data, valvec<byte_t>* record, EntropyContext* context) const;
        size_t decode_x8(fstring data, valvec<byte_t>* record, EntropyContext* context) const;

    private:
        template<size_t N>
        size_t decode_xN(fstring data, valvec<byte_t>* record, EntropyContext* context, bool check) const;

        struct Symbol {
            uint16_t start[256];
            uint16_t freq[256];
        };

        static void convert_symbol(const Rans64DecSymbol* from, Symbol* to);
        static byte_t ari(const Symbol* to, uint32_t m);

        Symbol syms_[257][257];
    };

}}