#include "entropy_base.hpp"
#include <memory>
#include <cmath>
#include <numeric>
#include <terark/bitmanip.hpp>

namespace terark {


ContextBuffer::~ContextBuffer() {
    if (c_ != nullptr && b_.capacity() >= sizeof(TerarkContext::BufferList) && b_.capacity() < (1ull << 20)) {
        auto node = reinterpret_cast<TerarkContext::BufferList*>(b_.data());
        node->c = b_.capacity();
        b_.risk_release_ownership();
        node->next = c_->list_;
        c_->list_ = node;
    }
}

ContextBuffer TerarkContext::alloc(size_t size) {
    size_t m = std::max(size, sizeof(BufferList));
    if (list_ == nullptr) {
        return ContextBuffer(valvec<byte_t>(size > 0 ? m : 0, valvec_reserve()), this);
    }
    BufferList **node = &list_;
    size_t c = list_->c;
    BufferList *n = list_;
    for (size_t r = size == 0 ? size_t(-1) : size; n->next != nullptr; n = n->next) {
        size_t nc = n->next->c;
        if (c >= r ? nc >= r && nc < c : nc > c) {
            node = &n->next;
            c = nc;
        }
    }
    n = *node;
    auto next = n->next;
    valvec<byte_t> b;
    b.risk_set_data(reinterpret_cast<byte_t*>(n));
    b.risk_set_capacity(n->c);
    b.ensure_capacity(m);
    *node = next;
    return ContextBuffer(std::move(b), this);
}

TerarkContext* GetTlsTerarkContext() {
    static thread_local TerarkContext tls_terark_ctx;
    return &tls_terark_ctx;
}

EntropyBytes EntropyBitsToBytes(EntropyBits* bits) {
    assert(bits->skip < 8);
    assert((bits->skip + bits->size) % 8 == 0);
    size_t size = (bits->skip + bits->size) / 8;
    assert(bits->data + size == bits->buffer.data() + bits->buffer.capacity());
    byte_t* ptr = bits->data;
    if (bits->skip == 0) {
        ++size;
        EntropyBitsReverseWriter<ContextBuffer>::prepare(&ptr, 1, &bits->buffer);
        *ptr = 0x80;
    }
    else {
        *ptr = (*ptr & ~((size_t(0xFF) >> (8 - bits->skip)))) | (1ull << (bits->skip - 1));
    }
    return EntropyBytes{ fstring(ptr, size), std::move(bits->buffer) };
}

EntropyBits EntropyBytesToBits(fstring bytes) {
    assert(bytes.size() > 0);
    assert(bytes[0] != 0);
    size_t skip = fast_ctz(uint32_t(bytes[0])) + 1;
    return { (byte_t*)bytes.udata(), skip, bytes.size() * 8 - skip, ContextBuffer() };
}

freq_hist::freq_hist(size_t min_len, size_t max_len) {
    min_ = min_len;
    max_ = max_len;
    clear();
}

void freq_hist::clear() {
    memset(&hist_, 0, sizeof hist_);
    memset(h1, 0, sizeof h1);
    memset(h2, 0, sizeof h2);
    memset(h3, 0, sizeof h3);
}

void freq_hist::normalise_hist(uint64_t* h, uint64_t& size, size_t normalise) {
    assert(std::accumulate(h, h + 256, size_t(0)) == size);
    if (size == 0) return;
    size_t t, i, s;
    byte_t seq[256];

    double p = ((double)normalise) / size;
    for (t = i = s = 0; i < 256; i++) {
        if (!h[i])
            continue;

        if ((h[i] *= p) == 0) h[i] = 1;

        t += h[i];
        ++s;
    }
    if (s == 0) return;
    if (s == 1)
        t++;
    if (t < normalise) {
        t = normalise - t;
        s = std::min<size_t>(t, 256);
        for (i = 0; i < 256; ++i) seq[i] = i;
        std::partial_sort(seq, seq + s, seq + 256, [=](byte_t l, byte_t r) {
            return h[l] > h[r];
        });
        for (i = 0; t > 0; i = (i + 1) % s) {
            if (h[seq[i]] > 0) {
                ++h[seq[i]];
                --t;
            }
        }
    }
    else if (t > normalise) {
        t -= normalise;
        s = std::min<size_t>(t, 256);
        for (i = 0; i < 256; ++i) seq[i] = i;
        std::partial_sort(seq, seq + s, seq + 256, [=](byte_t l, byte_t r) {
            uint64_t ll = h[l] <= 1 ? uint64_t(-1) : h[l];
            uint64_t rr = h[r] <= 1 ? uint64_t(-1) : h[r];
            return ll < rr;
        });
        for (i = 0; t > 0; i = (i + 1) % s) {
            if (h[seq[i]] > 1) {
                --h[seq[i]];
                --t;
            }
        }
    }
    size = normalise;
}

const freq_hist::histogram_t& freq_hist::histogram() const {
    return hist_;
}

size_t freq_hist::estimate_size(const histogram_t& hist) {
    double entropy = 0;
    double o0_size = hist.o0_size;
    for (size_t i = 0; i < 256; ++i) {
        if (hist.o0[i] > 0) {
            double p = hist.o0[i] / o0_size;
            entropy -= p * log2(p);
        }
    }
    return size_t(entropy * o0_size / 8);
}

void freq_hist::add_record(fstring record) {
    if (record.size() < min_ || record.size() > max_) {
        return;
    }
    hist_.o0_size += record.size();
    size_t idiv4 = record.size() / 4;

    const byte_t *in0 = record.udata();
    for (size_t i = 0, e = record.size() % 4; i < e; ++i) {
        ++hist_.o0[*in0++];
    }

    const byte_t *in1 = in0 + idiv4 * 1;
    const byte_t *in2 = in0 + idiv4 * 2;
    const byte_t *in3 = in0 + idiv4 * 3;

    const byte_t *in0_end = in1;
    while (in0 < in0_end) {
        ++hist_.o0[*in0++];
        ++h1[*in1++];
        ++h2[*in2++];
        ++h3[*in3++];
    }
}

void freq_hist::finish() {
    for (size_t i = 0; i < 256; ++i) {
        hist_.o0[i] += h1[i] + h2[i] + h3[i];
    }
}

void freq_hist::normalise(size_t norm) {
    assert(norm >= 256);
    normalise_hist(hist_.o0, hist_.o0_size, norm);
}

freq_hist_o1::freq_hist_o1(bool r1, size_t min_len, size_t max_len) {
    min_ = min_len;
    max_ = max_len;
    if (r1) {
        reset1();
    } else {
        clear();
    }
}

void freq_hist_o1::clear() {
    memset(&hist_, 0, sizeof hist_);
    memset(&o1_, 0, sizeof o1_);
}

void freq_hist_o1::reset1() {
    hist_.o0_size = 65536;
    auto ptr = (uint64_t*)hist_.o1;
    for (size_t i = 0; i < 65536; ++i) {
        ptr[i] = 1;
    }
    memset(&o1_, 0, sizeof o1_);
    memset(hist_.o1_size, 0, sizeof hist_.o1_size);
    memset(hist_.o0, 0, sizeof hist_.o0);
}

const freq_hist_o1::histogram_t& freq_hist_o1::histogram() const {
    return hist_;
}

size_t freq_hist_o1::estimate_size(const histogram_t& hist) {
    double entropy = 0;
    double o0_size = hist.o0_size;
    for (size_t i = 0; i < 256; ++i) {
        double o1_size = hist.o1_size[i];
        double pp = o1_size / o0_size;
        for (size_t j = 0; j < 256; ++j) {
            if (hist.o1[i][j] > 0) {
                double p = hist.o1[i][j] / o1_size;
                entropy -= pp * p * log2(p);
            }
        }
    }
    return size_t(entropy * o0_size / 8);
}

size_t freq_hist_o1::estimate_size_unfinish(const freq_hist_o1& freq) {
    auto& hist = freq.hist_;
    double entropy = 0;
    double o0_size = hist.o0_size;
    for (size_t i = 0; i < 256; ++i) {
        double o1_size = hist.o1_size[i];
        for (size_t j = 0; j < 256; ++j) {
            o1_size += hist.o1[i][j] + freq.o1_[i][j];
        }
        double pp = o1_size / o0_size;
        for (size_t j = 0; j < 256; ++j) {
            size_t o1_ij = hist.o1[i][j] + freq.o1_[i][j];
            if (o1_ij > 0) {
                double p = o1_ij / o1_size;
                entropy -= pp * p * log2(p);
            }
        }
    }
    return size_t(entropy * o0_size / 8);
}

size_t freq_hist_o1::estimate_size_unfinish(const freq_hist_o1& freq0, const freq_hist_o1& freq1) {
    auto& hist0 = freq0.hist_;
    auto& hist1 = freq1.hist_;
    double entropy = 0;
    double o0_size = hist0.o0_size + hist1.o0_size;
    for (size_t i = 0; i < 256; ++i) {
        double o1_size = hist0.o1_size[i] + hist1.o1_size[i];
        for (size_t j = 0; j < 256; ++j) {
            o1_size += hist0.o1[i][j] + hist1.o1[i][j] + freq0.o1_[i][j] + freq1.o1_[i][j];
        }
        double pp = o1_size / o0_size;
        for (size_t j = 0; j < 256; ++j) {
            uint64_t o1_ij = hist0.o1[i][j] + hist1.o1[i][j] + freq0.o1_[i][j] + freq1.o1_[i][j];
            if (o1_ij > 0) {
                double p = o1_ij / o1_size;
                entropy -= pp * p * log2(p);
            }
        }
    }
    return size_t(entropy * o0_size / 8);
}

void freq_hist_o1::add_record(fstring record) {
    if (record.size() < min_ || record.size() > max_) {
        return;
    }
    hist_.o0_size += record.size();
    byte_t c0, c1, c2, c3, c4, c5, c6, c7;
    byte_t l0, l1, l2, l3, l4, l5, l6, l7;

    if (record.size() < 1) {
        return;
    }
    const byte_t *in0 = record.udata();
    ++hist_.o0[l0 = *in0++];
    for (size_t i = 0, e = (record.size() - 1) % 8; i < e; ++i) {
        if (o1_[l0][c0 = *in0++]++ == 255) { hist_.o1[l0][c0] += 256; } l0 = c0;
    }
    size_t idiv8 = (record.size() - 1) / 8;
    if (idiv8 == 0) {
        return;
    }

    const byte_t *in1 = in0 + idiv8 * 1;
    const byte_t *in2 = in0 + idiv8 * 2;
    const byte_t *in3 = in0 + idiv8 * 3;
    const byte_t *in4 = in0 + idiv8 * 4;
    const byte_t *in5 = in0 + idiv8 * 5;
    const byte_t *in6 = in0 + idiv8 * 6;
    const byte_t *in7 = in0 + idiv8 * 7;

    l1 = in1[-1];
    l2 = in2[-1];
    l3 = in3[-1];
    l4 = in4[-1];
    l5 = in5[-1];
    l6 = in6[-1];
    l7 = in7[-1];

    const byte_t *in0_end = in1;
    while (in0 < in0_end) {
        if (o1_[l0][c0 = *in0++]++ == 255) { hist_.o1[l0][c0] += 256; } l0 = c0;
        if (o1_[l1][c1 = *in1++]++ == 255) { hist_.o1[l1][c1] += 256; } l1 = c1;
        if (o1_[l2][c2 = *in2++]++ == 255) { hist_.o1[l2][c2] += 256; } l2 = c2;
        if (o1_[l3][c3 = *in3++]++ == 255) { hist_.o1[l3][c3] += 256; } l3 = c3;
        if (o1_[l4][c4 = *in4++]++ == 255) { hist_.o1[l4][c4] += 256; } l4 = c4;
        if (o1_[l5][c5 = *in5++]++ == 255) { hist_.o1[l5][c5] += 256; } l5 = c5;
        if (o1_[l6][c6 = *in6++]++ == 255) { hist_.o1[l6][c6] += 256; } l6 = c6;
        if (o1_[l7][c7 = *in7++]++ == 255) { hist_.o1[l7][c7] += 256; } l7 = c7;
    }
}

void freq_hist_o1::add_hist(const freq_hist_o1& other) {
    hist_.o0_size += other.hist_.o0_size;
    for (size_t i = 0; i < 256; ++i) {
        hist_.o0[i] += other.hist_.o0[i];
        hist_.o1_size[i] += other.hist_.o1_size[i];
        for (size_t j = 0; j < 256; ++j) {
            hist_.o1[i][j] += other.hist_.o1[i][j] + other.o1_[i][j];
        }
    }
}

void freq_hist_o1::finish() {
    for (size_t i = 0; i < 256; ++i) {
        for (size_t j = 0; j < 256; ++j) {
            hist_.o1[i][j] += o1_[i][j];
            o1_[i][j] = 0;
            hist_.o0[j] += hist_.o1[i][j];
            hist_.o1_size[i] += hist_.o1[i][j];
        }
    }
}

void freq_hist_o1::normalise(size_t norm) {
    assert(norm >= 256);
    for (size_t i = 0; i < 256; ++i) {
        freq_hist::normalise_hist(hist_.o1[i], hist_.o1_size[i], norm);
    }
    freq_hist::normalise_hist(hist_.o0, hist_.o0_size, norm);
}

freq_hist_o2::freq_hist_o2(size_t min_len, size_t max_len) {
    min_ = min_len;
    max_ = max_len;
    clear();
}

void freq_hist_o2::clear() {
    memset(&hist_, 0, sizeof hist_);
}

const freq_hist_o2::histogram_t& freq_hist_o2::histogram() const {
    return hist_;
}

size_t freq_hist_o2::estimate_size(const histogram_t& hist) {
    double entropy = 0;
    double o0_size = hist.o0_size;
    for (size_t i = 0; i < 256; ++i) {
        for (size_t j = 0; j < 256; ++j) {
            double o2_size = hist.o2_size[i][j];
            for (size_t j = 0; j < 256; ++j) {
                for (size_t k = 0; k < 256; ++k) {
                    o2_size += hist.o2[i][j][k];
                }
            }
            double pp = o2_size / o0_size;
            for (size_t k = 0; k < 256; ++k) {
                if (hist.o2[i][j][k] > 0) {
                    double p = hist.o2[i][j][k] / o2_size;
                    entropy -= pp * p * log2(p);
                }
            }
        }
    }
    return size_t(entropy * o0_size / 8);
}

size_t freq_hist_o2::estimate_size_unfinish(const histogram_t& hist) {
    double entropy = 0;
    double o0_size = hist.o0_size;
    for (size_t i = 0; i < 256; ++i) {
        for (size_t j = 0; j < 256; ++j) {
            double o2_size = hist.o2_size[i][j];
            double pp = o2_size / o0_size;
            for (size_t k = 0; k < 256; ++k) {
                if (hist.o2[i][j][k] > 0) {
                    double p = hist.o2[i][j][k] / o2_size;
                    entropy -= pp * p * log2(p);
                }
            }
        }
    }
    return size_t(entropy * o0_size / 8);
}

void freq_hist_o2::add_record(fstring record) {
    if (record.size() < min_ || record.size() > max_) {
        return;
    }
    hist_.o0_size += record.size();
    byte_t c0, c1, c2, c3;
    byte_t last_00, last_01, last_02, last_03;
    byte_t last_10, last_11, last_12, last_13;

    if (record.size() < 1) {
        return;
    }
    const byte_t *in0 = record.udata();
    ++hist_.o0[last_00 = *in0++];
    if (record.size() < 2) {
        return;
    }
    last_10 = last_00;
    ++hist_.o1[last_10][last_00 = *in0++];
    for (size_t i = 0, e = (record.size() - 2) % 4; i < e; ++i) {
        ++hist_.o2[last_10][last_00][c0 = *in0++];
        last_10 = last_00;
        last_00 = c0;
    }
    size_t idiv4 = (record.size() - 2) / 4;
    if (idiv4 == 0) {
        return;
    }

    const byte_t *in1 = in0 + idiv4 * 1;
    const byte_t *in2 = in0 + idiv4 * 2;
    const byte_t *in3 = in0 + idiv4 * 3;

    const byte_t *in0_end = in1;

    last_01 = in1[-1];
    last_02 = in2[-1];
    last_03 = in3[-1];

    last_11 = in1[-2];
    last_12 = in2[-2];
    last_13 = in3[-2];


    do {
        ++hist_.o2[last_10][last_00][c0 = *in0++];
        last_10 = last_00;
        last_00 = c0;

        ++hist_.o2[last_11][last_01][c1 = *in1++];
        last_11 = last_01;
        last_01 = c1;

        ++hist_.o2[last_12][last_02][c2 = *in2++];
        last_12 = last_02;
        last_02 = c2;

        ++hist_.o2[last_13][last_03][c3 = *in3++];
        last_13 = last_03;
        last_03 = c3;
    } while (in0 < in0_end);
}

void freq_hist_o2::finish() {
    for (size_t i = 0; i < 256; ++i) {
        for (size_t j = 0; j < 256; ++j) {
            for (size_t k = 0; k < 256; ++k) {
                hist_.o1[j][k] += hist_.o2[i][j][k];
                hist_.o2_size[i][j] += hist_.o2[i][j][k];
            }
        }
    }
    for (size_t i = 0; i < 256; ++i) {
        for (size_t j = 0; j < 256; ++j) {
            hist_.o0[j] += hist_.o1[i][j];
            hist_.o1_size[i] += hist_.o1[i][j];
        }
    }
}

void freq_hist_o2::normalise(size_t norm) {
    assert(norm >= 256);
    for (size_t i = 0; i < 256; ++i) {
        for (size_t j = 0; j < 256; ++j) {
            freq_hist::normalise_hist(hist_.o2[i][j], hist_.o2_size[i][j], norm);
        }
        freq_hist::normalise_hist(hist_.o1[i], hist_.o1_size[i], norm);
    }
    freq_hist::normalise_hist(hist_.o0, hist_.o0_size, norm);
}

}
