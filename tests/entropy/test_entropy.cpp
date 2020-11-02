
#include <memory>
#include <terark/entropy/huffman_encoding.hpp>

using namespace terark;

int BUG_Huffman_decoder() {
    using namespace Huffman;
    // 63 zeros
    fstring raw_data = "000000000000000000000000000000000000000000000000000000000000000";

    freq_hist h;
    h.add_record(raw_data);
    h.finish();
    if (h.histogram().o0_size >= NORMALISE) {
        h.normalise(NORMALISE);
    }
    encoder e(h.histogram());
    auto encoded_data = e.encode(raw_data, GetTlsTerarkContext());
    decoder d(e.table());
    valvec<byte_t> record;
    auto success = d.decode(encoded_data.data, &record, GetTlsTerarkContext());
    if (!success) {
        return -1;
    }
    if (record.size() > record.capacity()) {
        return -2;
    }
    if (record != raw_data) {
        return -3;
    }
    return 0;
}


int BUG_Huffman_decoder_2() {
    using namespace Huffman;

    const char* table_char = "\377\001\000\003\001\000\002\177\200\247\200\002Y\020H\207\001\376\001\000\240\006\002@!\004\372\377\377\377\377\237\366\377\377\377\247\224R\245\224RJ)\245D)\245\224RJ)RJ)\245\224R\252\224RJ)\245\224R\372\377SJ)\245\224RJ)\245\224\242(\212RJ)\205\242(\212\242(\212\377\377(\372\377?\032\376\377\377\377\377\377";
    fstring table(table_char, 104);

    std::unique_ptr<decoder_o1> ptr(new decoder_o1(table));
    return 0;
}

int main(int argc, char* argv[]) {
    if (BUG_Huffman_decoder() != 0) {
        return -1;
    }
    if (BUG_Huffman_decoder_2() != 0) {
        return -1;
    }
    return 0;
}

