#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#define PFZD "Id"
#else
#define PFZD "zd"
#endif
#include <stdio.h>
#include <vector>
#include <functional>
#include <algorithm>
#include <random>
#include <terark/bitmap.hpp>
#include <terark/rank_select.hpp>

using namespace terark;

template<class BitVec>
size_t slow_rank1(const BitVec& bv, size_t bitpos) {
    assert(bitpos < bv.size());
    size_t nb = sizeof(bm_uint_t) * 8;
    size_t r = 0;
    for (size_t i = 0; i < bitpos/nb; ++i) {
        r += fast_popcount(bv.get_word(i));
    }
    if (bitpos % nb != 0) {
        r += fast_popcount_trail(bv.get_word(bitpos/nb), bitpos % nb);
    }
    return r;
}

template<class BitVec>
size_t slow_rank0(const BitVec& bv, size_t bitpos) {
    assert(bitpos < bv.size());
    return bitpos -= slow_rank1(bv, bitpos);
}

template<class BitVec>
size_t slow_select1(const BitVec& bv, size_t rank) {
    assert(rank < bv.size());
    size_t nb = sizeof(bm_uint_t) * 8;
    size_t rank2 = 0;
    for(size_t i = 0; i < bv.num_words(); ++i) {
        size_t upper = rank2 + fast_popcount(bv.get_word(i));
        if (rank < upper) {
            bm_uint_t w = bv.get_word(i);
            for (size_t j = 0; j < nb; ++j) {
                for (; rank2 < upper && (w & 1); ++j) {
                    if (rank2 == rank) {
                        size_t pos = i * nb + j;
                        assert(pos >= bv.size() || bv[pos]);
                        return std::min(pos, bv.size());
                    }
                    w >>= 1;
                    ++rank2;
                }
                w >>= 1;
            }
            return (i + 1) * nb;
        }
        rank2 = upper;
    }
    THROW_STD(invalid_argument, "rank=%" PFZD " is too large", rank);
}

template<class BitVec>
size_t slow_select0(const BitVec& bv, size_t rank) {
    assert(rank < bv.size());
    size_t nb = sizeof(bm_uint_t) * 8;
    size_t rank2 = 0;
    for(size_t i = 0; i < bv.num_words(); ++i) {
        size_t upper = rank2 + nb - fast_popcount(bv.get_word(i));
        if (rank < upper) {
            bm_uint_t w = bv.get_word(i);
            for (size_t j = 0; j < nb; ++j) {
                for (; rank2 < upper && !(w & 1); ++j) {
                    if (rank2 == rank) {
                        size_t pos = i * nb + j;
                        assert(pos >= bv.size() || !bv[pos]);
                        return std::min(pos, bv.size());
                    }
                    w >>= 1;
                    ++rank2;
                }
                w >>= 1;
            }
            return (i + 1) * nb;
        }
        rank2 = upper;
    }
    THROW_STD(invalid_argument, "rank=%" PFZD " is too large", rank);
}

std::mt19937_64 mt;

inline bm_uint_t rand_word() {
    return mt();
}

template<class RsBitVec>
void test_rs(RsBitVec& rs) {
    auto bldata = rs.bldata();
    auto rank = rs.get_rank_cache();
    auto sel0 = rs.get_sel0_cache();
    auto sel1 = rs.get_sel1_cache();
    for(size_t i = 0; i < rs.size(); ++i) {
        size_t ffff_i0 = RsBitVec::fast_is0(bldata, i);
        size_t fast_i0 = rs.is0(i);
        size_t ffff_i1 = RsBitVec::fast_is1(bldata, i);
        size_t fast_i1 = rs.is1(i);
        if (!ffff_i0 ^ !fast_i0) {
            printf("%06" PFZD " ffff_i0=%06" PFZD " fast_i0=%06" PFZD "\n"
                , i, ffff_i0, fast_i0);
        }
        if (!ffff_i1 ^ !fast_i1) {
            printf("%06" PFZD " ffff_i1=%06" PFZD " fast_i1=%06" PFZD "\n"
                , i, ffff_i1, fast_i1);
        }
        assert(ffff_i0 == fast_i0);
        assert(ffff_i1 == fast_i1);
    }
    for(size_t i = 0; i < rs.size(); ++i) {
        size_t ffff_r0 = RsBitVec::fast_rank0(bldata, rank, i);
        size_t fast_r0 = rs.rank0(i);
        size_t slow_r0 = slow_rank0(rs, i);
        if (ffff_r0 != slow_r0) {
            printf("%06" PFZD " ffff_r0=%06" PFZD " slow_r0=%06" PFZD "\n"
                , i, ffff_r0, slow_r0);
        }
        if (fast_r0 != slow_r0) {
            printf("%06" PFZD " fast_r0=%06" PFZD " slow_r0=%06" PFZD "\n"
                , i, fast_r0, slow_r0);
        }
        assert(ffff_r0 <= rs.max_rank0());
        assert(fast_r0 <= rs.max_rank0());
        assert(slow_r0 <= rs.max_rank0());
        assert(ffff_r0 == slow_r0);
        assert(fast_r0 == slow_r0);
    }
    for(size_t i = 0; i < rs.size(); ++i) {
        size_t ffff_r1 = RsBitVec::fast_rank1(bldata, rank, i);
        size_t fast_r1 = rs.rank1(i);
        size_t slow_r1 = slow_rank1(rs, i);
        if (ffff_r1 != slow_r1) {
            printf("%06" PFZD " ffff_r1=%06" PFZD " slow_r1=%06" PFZD "\n"
                , i, ffff_r1, slow_r1);
        }
        if (fast_r1 != slow_r1) {
            printf("%06" PFZD " fast_r1=%06" PFZD " slow_r1=%06" PFZD "\n"
                , i, fast_r1, slow_r1);
        }
        assert(ffff_r1 <= rs.max_rank1());
        assert(fast_r1 <= rs.max_rank1());
        assert(slow_r1 <= rs.max_rank1());
        assert(ffff_r1 == slow_r1);
        assert(fast_r1 == slow_r1);
    }
    for(size_t i = 0; i < rs.max_rank0(); ++i) {
        size_t ffff_s0 = RsBitVec::fast_select0(bldata, sel0, rank, i);
        size_t fast_s0 = rs.select0(i);
        size_t slow_s0 = slow_select0(rs, i);
        if (ffff_s0 != slow_s0) {
            printf("%06" PFZD " ffff_s0=%06" PFZD " slow_s0=%06" PFZD "\n"
                , i, ffff_s0, slow_s0);
        }
        if (fast_s0 != slow_s0) {
            printf("%06" PFZD " fast_s0=%06" PFZD " slow_s0=%06" PFZD "\n"
                , i, fast_s0, slow_s0);
        }
        assert(ffff_s0 < rs.size());
        assert(fast_s0 < rs.size());
        assert(slow_s0 < rs.size());
        assert(ffff_s0 == slow_s0);
        assert(fast_s0 == slow_s0);
    }
    for(size_t i = 0; i < rs.max_rank1(); ++i) {
        size_t ffff_s1 = RsBitVec::fast_select1(bldata, sel1, rank, i);
        size_t fast_s1 = rs.select1(i);
        size_t slow_s1 = slow_select1(rs, i);
        if (ffff_s1 != slow_s1) {
            printf("%06" PFZD " ffff_s1=%06" PFZD " slow_s1=%06" PFZD "\n"
                , i, ffff_s1, slow_s1);
        }
        if (fast_s1 != slow_s1) {
            printf("%06" PFZD " fast_s1=%06" PFZD " slow_s1=%06" PFZD "\n"
                , i, fast_s1, slow_s1);
        }
        assert(ffff_s1 < rs.size());
        assert(fast_s1 < rs.size());
        assert(slow_s1 < rs.size());
        assert(ffff_s1 == slow_s1);
        assert(fast_s1 == slow_s1);
    }
}

template<>
void test_rs<rank_select_simple>(rank_select_simple& rs) {
    for(size_t i = 0; i < rs.size(); ++i) {
        size_t fast_r0 = rs.rank0(i);
        size_t slow_r0 = slow_rank0(rs, i);
        if (fast_r0 != slow_r0) {
            printf("%06" PFZD " fast_r0=%06" PFZD " slow_r0=%06" PFZD "\n"
                , i, fast_r0, slow_r0);
        }
        assert(fast_r0 <= rs.max_rank0());
        assert(slow_r0 <= rs.max_rank0());
        assert(fast_r0 == slow_r0);
    }
    for(size_t i = 0; i < rs.size(); ++i) {
        size_t fast_r1 = rs.rank1(i);
        size_t slow_r1 = slow_rank1(rs, i);
        if (fast_r1 != slow_r1) {
            printf("%06" PFZD " fast_r1=%06" PFZD " slow_r1=%06" PFZD "\n"
                , i, fast_r1, slow_r1);
        }
        assert(fast_r1 <= rs.max_rank1());
        assert(slow_r1 <= rs.max_rank1());
        assert(fast_r1 == slow_r1);
    }
    for(size_t i = 0; i < rs.max_rank0(); ++i) {
        size_t fast_s0 = rs.select0(i);
        size_t slow_s0 = slow_select0(rs, i);
        if (fast_s0 != slow_s0) {
            printf("%06" PFZD " fast_s0=%06" PFZD " slow_s0=%06" PFZD "\n"
                , i, fast_s0, slow_s0);
        }
        assert(fast_s0 < rs.size());
        assert(slow_s0 < rs.size());
        assert(fast_s0 == slow_s0);
    }
    for(size_t i = 0; i < rs.max_rank1(); ++i) {
        size_t fast_s1 = rs.select1(i);
        size_t slow_s1 = slow_select1(rs, i);
        if (fast_s1 != slow_s1) {
            printf("%06" PFZD " fast_s1=%06" PFZD " slow_s1=%06" PFZD "\n"
                , i, fast_s1, slow_s1);
        }
        assert(fast_s1 < rs.size());
        assert(slow_s1 < rs.size());
        assert(fast_s1 == slow_s1);
    }
}

template<class RsBitVec>
void test(size_t max_bits) {
    RsBitVec  rs(max_bits, valvec_no_init());
    for (size_t i = 0; i < rs.num_words(); ++i) {
        rs.set_word(i, rand_word());
        if (rs.get_word(i) % 4 == 0) {
            rs.set_word(i, size_t(-1));
        }
        if (rand() % 5 == 1) {
            rs.set_word(i, size_t(0));
        }
    //    printf("%06" PFZD "'th rand = %016zX\n", i, rs.bldata()[i]);
    }
    rs.push_back(true);
    rs.push_back(true);
    rs.push_back(false);
    for(size_t i = 0; i < rs.size(); ++i) {
        size_t fast_one_seq_len = std::min(rs.one_seq_len(i), rs.size() - i);
        size_t slow_one_seq_len = 0;
        while (slow_one_seq_len + i < rs.size() && rs[slow_one_seq_len + i])
            ++slow_one_seq_len;
        if (slow_one_seq_len != fast_one_seq_len) {
            printf("%06" PFZD " slow_one_seq_len=%06" PFZD " fast_one_seq_len=%06" PFZD "\n"
                , i, slow_one_seq_len, fast_one_seq_len);
        }
        assert(slow_one_seq_len == fast_one_seq_len);
    }
    for(size_t i = 0; i <= rs.size(); ++i) {
        size_t fast_one_seq_revlen = rs.one_seq_revlen(i);
        size_t slow_one_seq_revlen = 0;
        while (i > slow_one_seq_revlen && rs[i - slow_one_seq_revlen - 1])
            ++slow_one_seq_revlen;
        if (slow_one_seq_revlen != fast_one_seq_revlen) {
            printf("%06" PFZD " slow_one_seq_revlen=%06" PFZD " fast_one_seq_revlen=%06" PFZD "\n"
                , i, fast_one_seq_revlen, fast_one_seq_revlen);
        }
        assert(slow_one_seq_revlen == fast_one_seq_revlen);
    }
    for(size_t i = 0; i < rs.size(); ++i) {
        size_t fast_zero_seq_len = std::min(rs.zero_seq_len(i), rs.size() - i);
        size_t slow_zero_seq_len = 0;
        while (slow_zero_seq_len + i < rs.size() && !rs[slow_zero_seq_len + i])
            ++slow_zero_seq_len;
        if (slow_zero_seq_len != fast_zero_seq_len) {
            printf("%06" PFZD " slow_zero_seq_len=%06" PFZD " fast_zero_seq_len=%06" PFZD "\n"
                , i, slow_zero_seq_len, fast_zero_seq_len);
        }
        assert(slow_zero_seq_len == fast_zero_seq_len);
    }
    for (size_t i = 0; i <= rs.size(); ++i) {
        size_t fast_zero_seq_revlen = rs.zero_seq_revlen(i);
        size_t slow_zero_seq_revlen = 0;
        while (i > slow_zero_seq_revlen && !rs[i - slow_zero_seq_revlen - 1])
            ++slow_zero_seq_revlen;
        if (slow_zero_seq_revlen != fast_zero_seq_revlen) {
            printf("%06" PFZD " slow_zero_seq_revlen=%06" PFZD " fast_zero_seq_revlen=%06" PFZD "\n"
                , i, fast_zero_seq_revlen, fast_zero_seq_revlen);
        }
        assert(slow_zero_seq_revlen == fast_zero_seq_revlen);
    }
    rs.build_cache(true, true);
    test_rs(rs);
    RsBitVec rs2;
    rs2.risk_mmap_from((unsigned char*)rs.data(), rs.mem_size());
    test_rs(rs2);
    rs2.risk_release_ownership();
}

template<class RsBitVec, size_t Index>
void init_mixed(RsBitVec& rs_base) {
    auto &rs = rs_base.template get<Index>();
    for (size_t i = 0; i < rs.num_words(); ++i) {
        rs.set_word(i, rand_word());
        if (rs.get_word(i) % 4 == 0) {
            rs.set_word(i, size_t(-1));
        }
        if (rand() % 5 == 1) {
            rs.set_word(i, size_t(0));
        }
        //    printf("%06" PFZD "'th rand = %016zX\n", i, rs.bldata()[i]);
    }
    rs.push_back(!(rand() & 1));
    rs.push_back(!(rand() & 1));
    rs.push_back(!(rand() & 1));
    for (size_t i = 0; i < rs.size(); ++i) {
        size_t fast_one_seq_len = std::min(rs.one_seq_len(i), rs.size() - i);
        size_t slow_one_seq_len = 0;
        while (slow_one_seq_len + i < rs.size() && rs[slow_one_seq_len + i])
            ++slow_one_seq_len;
        if (slow_one_seq_len != fast_one_seq_len) {
            printf("%06" PFZD " slow_one_seq_len=%06" PFZD " fast_one_seq_len=%06" PFZD "\n"
                , i, slow_one_seq_len, fast_one_seq_len);
        }
        assert(slow_one_seq_len == fast_one_seq_len);
    }
    for (size_t i = 0; i <= rs.size(); ++i) {
        size_t fast_one_seq_revlen = rs.one_seq_revlen(i);
        size_t slow_one_seq_revlen = 0;
        while (i > slow_one_seq_revlen && rs[i - slow_one_seq_revlen - 1])
            ++slow_one_seq_revlen;
        if (slow_one_seq_revlen != fast_one_seq_revlen) {
            printf("%06" PFZD " slow_one_seq_revlen=%06" PFZD " fast_one_seq_revlen=%06" PFZD "\n"
                , i, fast_one_seq_revlen, fast_one_seq_revlen);
        }
        assert(slow_one_seq_revlen == fast_one_seq_revlen);
    }
    for (size_t i = 0; i < rs.size(); ++i) {
        size_t fast_zero_seq_len = std::min(rs.zero_seq_len(i), rs.size() - i);
        size_t slow_zero_seq_len = 0;
        while (slow_zero_seq_len + i < rs.size() && !rs[slow_zero_seq_len + i])
            ++slow_zero_seq_len;
        if (slow_zero_seq_len != fast_zero_seq_len) {
            printf("%06" PFZD " slow_zero_seq_len=%06" PFZD " fast_zero_seq_len=%06" PFZD "\n"
                , i, slow_zero_seq_len, fast_zero_seq_len);
        }
        assert(slow_zero_seq_len == fast_zero_seq_len);
    }
    for (size_t i = 0; i <= rs.size(); ++i) {
        size_t fast_zero_seq_revlen = rs.zero_seq_revlen(i);
        size_t slow_zero_seq_revlen = 0;
        while (i > slow_zero_seq_revlen && !rs[i - slow_zero_seq_revlen - 1])
            ++slow_zero_seq_revlen;
        if (slow_zero_seq_revlen != fast_zero_seq_revlen) {
            printf("%06" PFZD " slow_zero_seq_revlen=%06" PFZD " fast_zero_seq_revlen=%06" PFZD "\n"
                , i, fast_zero_seq_revlen, fast_zero_seq_revlen);
        }
        assert(slow_zero_seq_revlen == fast_zero_seq_revlen);
    }
    rs.build_cache(true, true);
    test_rs(rs);
    RsBitVec rs2;
    rs2.risk_mmap_from((unsigned char*)rs_base.data(), rs_base.mem_size());
    test_rs(rs2.template get<Index>());
    rs2.risk_release_ownership();
}

template<class RsBitVec, size_t Arity, size_t I>
struct test_mixed_dimensions {
    static void init(RsBitVec& rs, std::vector<std::function<void()>> &init) {
        init.emplace_back([&] { init_mixed<RsBitVec, I>(rs); });
        test_mixed_dimensions<RsBitVec, Arity, I + 1>::init(rs, init);
    }
    static void test(RsBitVec& rs) {
        test_rs(rs.template get<I>());
        test_mixed_dimensions<RsBitVec, Arity, I + 1>::test(rs);
    }
};
template<class RsBitVec, size_t Arity>
struct test_mixed_dimensions<RsBitVec, Arity, Arity> {
    static void init(RsBitVec& rs_base, std::vector<std::function<void()>> &init) {
    }
    static void test(RsBitVec& rs) {
    }
};

template<class RsBitVec, size_t Arity>
void test_mixed(size_t max_bits) {
    RsBitVec rs_base(max_bits, valvec_no_init());
    std::vector<std::function<void()>> init;
    test_mixed_dimensions<RsBitVec, Arity, 0>::init(rs_base, init);
    std::shuffle(init.begin(), init.end(), mt);
    for (auto& f : init) {
        f();
    }

    test_mixed_dimensions<RsBitVec, Arity, 0>::test(rs_base);
    RsBitVec rs2;
    rs2.risk_mmap_from((unsigned char*)rs_base.data(), rs_base.mem_size());
    test_mixed_dimensions<RsBitVec, Arity, 0>::test(rs2);
    rs2.risk_release_ownership();
}

int main(int argc, char* argv[]) {
    size_t max_bits = 10000;
    if (argc < 2) {
        fprintf(stderr, "usage: %s num_max_bits(default=10000)\n", argv[0]);
    }
    else {
        max_bits = strtoul(argv[1], NULL, 10);
    }
    assert(UintSelect1(uint64_t(1) <<  0, 0) ==  0);
    assert(UintSelect1(uint64_t(1) <<  1, 0) ==  1);
    assert(UintSelect1(uint64_t(1) <<  2, 0) ==  2);
    assert(UintSelect1(uint64_t(1) <<  3, 0) ==  3);
    assert(UintSelect1(uint64_t(1) << 30, 0) == 30);
    assert(UintSelect1(uint64_t(1) << 31, 0) == 31);
    assert(UintSelect1(uint64_t(1) << 32, 0) == 32);
    assert(UintSelect1(uint64_t(1) << 33, 0) == 33);
    assert(UintSelect1(uint64_t(1) << 63, 0) == 63);
    mt.seed(max_bits);

    test<rank_select_simple   >(max_bits);
    test<rank_select_il       >(max_bits);
    test<rank_select_se       >(max_bits);
    test<rank_select_se_512   >(max_bits);
    test<rank_select_se_512_64>(max_bits);

    test_mixed<rank_select_mixed_il_256   , 2>(max_bits);
    test_mixed<rank_select_mixed_se_512   , 2>(max_bits);
    test_mixed<rank_select_mixed_xl_256<2>, 2>(max_bits);
    test_mixed<rank_select_mixed_xl_256<3>, 3>(max_bits);
    test_mixed<rank_select_mixed_xl_256<4>, 4>(max_bits);

    fprintf(stderr, "All Passed!\n");
    return 0;
}

