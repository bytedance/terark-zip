#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#else
#endif
#include <stdio.h>
#include <terark/bitmap.hpp>
#include <terark/rank_select.hpp>

#if defined(_MSC_VER)
#   pragma warning(disable:4819)
#   include <intrin.h>
#   pragma intrinsic(_BitScanForward64)
#   pragma intrinsic(_BitScanReverse64)

#   define MSVC_COMPILER
#   include <iso646.h>
#   define __builtin_popcountll __popcnt64
    inline int __builtin_ctzll(unsigned __int64 x) {
        assert(0 != x);
        unsigned long c = 0;
        _BitScanForward64(&c, x);
        return c;
    }
    inline int __builtin_clzll(unsigned __int64 x) {
        assert(0 != x);
        unsigned long c = 0;
        _BitScanReverse64(&c, x);
        return c;
    }
#endif
#include <sdsl/rank_support.hpp>
#include <sdsl/select_support.hpp>
#include <sdsl/bit_vectors.hpp>
#include "../../../sdsl-lite/lib/bits.cpp"
#include "../../../sdsl-lite/lib/io.cpp"
#include "../../../sdsl-lite/lib/memory_management.cpp"
#include "../../../sdsl-lite/lib/ram_filebuf.cpp"
#include "../../../sdsl-lite/lib/ram_fs.cpp"
#include "../../../sdsl-lite/lib/sfstream.cpp"
#include "../../../sdsl-lite/lib/util.cpp"

using namespace terark;


template<class T, bool FAST, bool UNROLL = false>
struct test_rank_select {
    test_rank_select(std::string name, size_t count, size_t size) {
        ro = std::chrono::seconds(0);
        so = std::chrono::seconds(0);
        rr = std::chrono::seconds(0);
        sr = std::chrono::seconds(0);
        std::mt19937_64 mt(0);
        size_t sum = 0;
        size_t final_count = 0;
        double total_size = 0;
        for (size_t i = 0; i < 8; ++i) {
            size_t sub_count = (count + 7) / 8 + size_t(mt() & 64ULL);
            total_size += test(sub_count, size, mt, sum, std::integral_constant<bool, FAST>());
            final_count += sub_count;
        }
        total_size /= 8;
        fprintf(stderr,
                "%s\t"
                "%7.3f\t"   //rank_ordered
                "%7.3f\t"   //select_ordered
                "%7.3f\t"   //rank_random
                "%7.3f\t"   //select_random
                "%12zd\t"   //bit_count
                "%12zd\t"   //bit_size
                "%12.1f\t"  //total_size
                "%7.3f\t"   //expand_rate
                "%016zX\n"   //check_sum
                , name.c_str(),
                ro.count() / final_count,
                so.count() / final_count,
                rr.count() / final_count,
                sr.count() / final_count,
                size,
                (size + 7) / 8,
                total_size,
                total_size / ((size + 7) / 8),
                sum
        );
    }
    static auto do_test(std::function<size_t()> f) {
        auto begin = std::chrono::high_resolution_clock::now();
        size_t sum = f();
        TERARK_UNUSED_VAR(sum);
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::duration<double, std::nano>>(end - begin);
    }
    size_t test(size_t count, size_t size, std::mt19937_64& mt, size_t &sum, std::false_type) {
        std::vector<size_t> r;
        r.resize(count);
        T t;
        t.init(mt, size);
        size_t max_rank = t.max_rank();
        if (max_rank == 0)
            throw 0;
        ro += do_test([&, count, size] {
            size_t s = 0;
            for(size_t c = 0; c < count; ) {
                size_t limit = std::min(count - c, size);
                size_t i = 0;
                if (UNROLL) {
                    for (; i + 3 < limit; i += 4)
                        s += t.rank(i) + t.rank(i + 1) + t.rank(i + 2) + t.rank(i + 3);
                }
                for (; i < limit; ++i)
                    s += t.rank(i);
                c += limit;
            }
            return sum += s;
        });
        so += do_test([&, count, max_rank] {
            size_t s = 0;
            for(size_t c = 0; c < count; ) {
                size_t limit = std::min(count - c, max_rank);
                size_t i = 0;
                if (UNROLL) {
                    for (; i + 3 < limit; i += 4)
                        s += t.select(i) + t.select(i + 1) + t.select(i + 2) + t.select(i + 3);
                }
                for (; i < limit; ++i)
                    s += t.select(i);
                c += limit;
            }
            return sum += s;
        });
        std::generate(r.begin(), r.end(), std::bind(std::uniform_int_distribution<size_t>(0, size - 1), std::ref(mt)));
        rr += do_test([&, count] {
            size_t s = 0;
            size_t i = 0;
            if (UNROLL) {
                for(; i + 3 < count; i += 4)
                    s += t.rank(r[i]) + t.rank(r[i + 1]) + t.rank(r[i + 2]) + t.rank(r[i + 3]);
            }
            for(; i < count; ++i)
                s += t.rank(r[i]);
            return sum += s;
        });
        std::generate(r.begin(), r.end(), std::bind(std::uniform_int_distribution<size_t>(0, max_rank - 1), std::ref(mt)));
        sr += do_test([&, count] {
            size_t s = 0;
            size_t i = 0;
            if (UNROLL) {
                for(; i + 3 < count; i += 4)
                    s += t.select(r[i]) + t.select(r[i + 1]) + t.select(r[i + 2]) + t.select(r[i + 3]);
            }
            for(; i < count; ++i)
                s += t.select(r[i]);
            return sum += s;
        });
        return t.total_size();
    }
    size_t test(size_t count, size_t size, std::mt19937_64& mt, size_t &sum, std::true_type) {
        std::vector<size_t> r;
        r.resize(count);
        T t;
        t.init(mt, size);
        size_t max_rank = t.max_rank();
        if (max_rank == 0)
            throw 0;
        ro += do_test([&, count, size] {
            auto bldata = t.bldata();
            auto rank = t.get_rank_cache();
            size_t s = 0;
            for(size_t c = 0; c < count; ) {
                size_t limit = std::min(count - c, size);
                size_t i = 0;
                if (UNROLL) {
                    for (; i + 3 < limit; i += 4)
                        s += T::fast_rank(bldata, rank, i + 0)
                          +  T::fast_rank(bldata, rank, i + 1)
                          +  T::fast_rank(bldata, rank, i + 2)
                          +  T::fast_rank(bldata, rank, i + 3);
                }
                for (; i < limit; ++i)
                    s += T::fast_rank(bldata, rank, i);
                c += limit;
            }
            return sum += s;
        });
        so += do_test([&, count, max_rank] {
            auto bldata = t.bldata();
            auto sel = t.get_sel_cache();
            auto rank = t.get_rank_cache();
            size_t s = 0;
            for(size_t c = 0; c < count; ) {
                size_t limit = std::min(count - c, max_rank);
                size_t i = 0;
                if (UNROLL) {
                    for (; i + 3 < limit; i += 4)
                        s += T::fast_select(bldata, sel, rank, i + 0)
                          +  T::fast_select(bldata, sel, rank, i + 1)
                          +  T::fast_select(bldata, sel, rank, i + 2)
                          +  T::fast_select(bldata, sel, rank, i + 3);
                }
                for (; i < limit; ++i)
                    s += T::fast_select(bldata, sel, rank, i);
                c += limit;
            }
            return sum += s;
        });
        std::generate(r.begin(), r.end(), std::bind(std::uniform_int_distribution<size_t>(0, size - 1), std::ref(mt)));
        rr += do_test([&, count] {
            auto bldata = t.bldata();
            auto rank = t.get_rank_cache();
            size_t s = 0;
            size_t i = 0;
            if(UNROLL) {
                for(; i + 3 < count; i += 4)
                    s += T::fast_rank(bldata, rank, r[i + 0])
                    + T::fast_rank(bldata, rank, r[i + 1])
                    + T::fast_rank(bldata, rank, r[i + 2])
                    + T::fast_rank(bldata, rank, r[i + 3]);
            }
            for(; i < count; ++i)
                s += T::fast_rank(bldata, rank, r[i]);
            return sum += s;
        });
        std::generate(r.begin(), r.end(), std::bind(std::uniform_int_distribution<size_t>(0, max_rank - 1), std::ref(mt)));
        sr += do_test([&, count] {
            auto bldata = t.bldata();
            auto sel = t.get_sel_cache();
            auto rank = t.get_rank_cache();
            size_t s = 0;
            size_t i = 0;
            if (UNROLL) {
                for(; i + 3 < count; i += 4)
                      s += T::fast_select(bldata, sel, rank, r[i + 0])
                        +  T::fast_select(bldata, sel, rank, r[i + 1])
                        +  T::fast_select(bldata, sel, rank, r[i + 2])
                        +  T::fast_select(bldata, sel, rank, r[i + 3]);
            }
            for(; i < count; ++i)
                s += T::fast_select(bldata, sel, rank, r[i]);
            return sum += s;
        });
        return t.total_size();
    }
    std::chrono::duration<double, std::nano> ro;
    std::chrono::duration<double, std::nano> so;
    std::chrono::duration<double, std::nano> rr;
    std::chrono::duration<double, std::nano> sr;
};

struct sdsl_entity {
    sdsl::bit_vector bv;
    sdsl::rank_support_v<> r;
    sdsl::select_support_mcl<> s;
    size_t r_max;

    void init(std::mt19937_64& mt, size_t size) {
        bv.resize(size);
        for (size_t i = 0; i < size; i += 64) {
            bv.set_int(i, mt());
            if (bv.get_int(i) % 4 == 0)
                bv.set_int(i, size_t(-1));
            if (mt() % 5 == 1)
                bv.set_int(i, size_t(0));
        }
        r = sdsl::rank_support_v<>(&bv);
        s = sdsl::select_support_mcl<>(&bv);
        r_max = sdsl::select_support_trait<1, 1>::arg_cnt(bv);
    }
    size_t max_rank() const {
        return r_max;
    }
    size_t total_size() const {
        return (bv.size() + 63) / 64 * 8 + (bv.size() + 511) / 512 * 16 + (bv.size() + 4095) / 4096 * 64 * 3;
    }

    size_t rank(size_t i) const {
        return r.rank(i);
    }
    size_t select(size_t i) const {
        return s.select(i + 1);
    }
};

template<class T>
struct terark_entity {
    T rs;

    void init(std::mt19937_64& mt, size_t size) {
        rs.resize(size);
        for (size_t i = 0, max = (size + 63) / 64; i < max; ++i) {
            rs.set_word(i, mt());
            if (rs.get_word(i) % 4 == 0)
                rs.set_word(i, size_t(-1));
            if (mt() % 5 == 1)
                rs.set_word(i, size_t(0));
        }
        rs.build_cache(false, true);
    }
    decltype(auto) bldata() const {
        return rs.bldata();
    }
    decltype(auto) get_rank_cache() const {
        return rs.get_rank_cache();
    }
    decltype(auto) get_sel_cache() const {
        return rs.get_sel1_cache();
    }
    size_t max_rank() const {
        return rs.max_rank1();
    }
    size_t total_size() const {
        return rs.mem_size();
    }

    inline size_t rank(size_t i) const {
        return rs.rank1(i);
    }
    template<class bldata_t, class rank_t>
    static inline size_t fast_rank(bldata_t bldata, rank_t rank, size_t i) {
        return T::fast_rank1(bldata, rank, i);
    }
    inline size_t select(size_t i) const {
        return rs.select1(i);
    }
    template<class bldata_t, class sel_t, class rank_t>
    static inline size_t fast_select(bldata_t bldata, sel_t sel, rank_t rank, size_t i) {
        return T::fast_select1(bldata, sel, rank, i);
    }
};

int main(int argc, char* argv[]) {
    fprintf(stderr,
            "name\t"
            "rank_ordered\t"
            "select_ordered\t"
            "rank_random\t"
            "select_random\t"
            "bit_count\t"
            "bit_bytes\t"
            "total_bytes\t"
            "expand_ratio\t"
            "check_sum\n");
    fprintf(stderr, "\n");

    //test_rank_select<  sdsl_entity                       , false>("sdsl_v_mcl"    , 15, 71);
    //test_rank_select<terark_entity<rank_select_se_256_32>, false>("se_256_32"     , 15, 71);
    //test_rank_select<terark_entity<rank_select_se_256_32>, true >("se_256_32_fast", 15, 71);
    //test_rank_select<terark_entity<rank_select_il_256_32>, false>("il_256_32"     , 15, 71);
    //test_rank_select<terark_entity<rank_select_il_256_32>, true >("il_256_32_fast", 15, 71);
    //test_rank_select<terark_entity<rank_select_se_512_32>, false>("se_512_32"     , 15, 71);
    //test_rank_select<terark_entity<rank_select_se_512_32>, true >("se_512_32_fast", 15, 71);
    //test_rank_select<terark_entity<rank_select_se_512_64>, false>("se_512_64"     , 15, 71);
    //test_rank_select<terark_entity<rank_select_se_512_64>, true >("se_512_64_fast", 15, 71);
    //return 0;
    size_t count = 100000, maxMB = 512;
    if (argc > 1)
        count = (size_t)strtoull(argv[1], NULL, 10);
    if (argc > 2)
        maxMB = (size_t)strtoull(argv[2], NULL, 10);

if (maxMB > 4) {
    test_rank_select<  sdsl_entity                       , false>("sdsl_v_mcl"    , count, 8ULL * 1024 * 1024 * maxMB);
    test_rank_select<terark_entity<rank_select_se_512_32>, false>("se_512_32"     , count, 8ULL * 1024 * 1024 * maxMB);
    test_rank_select<terark_entity<rank_select_se_512_32>, true >("se_512_32_fast", count, 8ULL * 1024 * 1024 * maxMB);
    test_rank_select<terark_entity<rank_select_se_512_64>, false>("se_512_64"     , count, 8ULL * 1024 * 1024 * maxMB);
    test_rank_select<terark_entity<rank_select_se_512_64>, true >("se_512_64_fast", count, 8ULL * 1024 * 1024 * maxMB);
    test_rank_select<terark_entity<rank_select_se_256_32>, false>("se_256_32"     , count, 8ULL * 1024 * 1024 * maxMB);
    test_rank_select<terark_entity<rank_select_se_256_32>, true >("se_256_32_fast", count, 8ULL * 1024 * 1024 * maxMB);
    test_rank_select<terark_entity<rank_select_il_256_32>, false>("il_256_32"     , count, 8ULL * 1024 * 1024 * maxMB);
    test_rank_select<terark_entity<rank_select_il_256_32>, true >("il_256_32_fast", count, 8ULL * 1024 * 1024 * maxMB);
    fprintf(stderr, "\n");
}
    test_rank_select<  sdsl_entity                       , false>("sdsl_v_mcl"    , count, 8ULL * 1024 * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_512_32>, false>("se_512_32"     , count, 8ULL * 1024 * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_512_32>, true >("se_512_32_fast", count, 8ULL * 1024 * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_512_64>, false>("se_512_64"     , count, 8ULL * 1024 * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_512_64>, true >("se_512_64_fast", count, 8ULL * 1024 * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_256_32>, false>("se_256_32"     , count, 8ULL * 1024 * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_256_32>, true >("se_256_32_fast", count, 8ULL * 1024 * 1024 * 4);
    test_rank_select<terark_entity<rank_select_il_256_32>, false>("il_256_32"     , count, 8ULL * 1024 * 1024 * 4);
    test_rank_select<terark_entity<rank_select_il_256_32>, true >("il_256_32_fast", count, 8ULL * 1024 * 1024 * 4);
    fprintf(stderr, "\n");
    test_rank_select<  sdsl_entity                       , false>("sdsl_v_mcl"    , count, 8ULL * 1024 * 128);
    test_rank_select<terark_entity<rank_select_se_512_32>, false>("se_512_32"     , count, 8ULL * 1024 * 128);
    test_rank_select<terark_entity<rank_select_se_512_32>, true >("se_512_32_fast", count, 8ULL * 1024 * 128);
    test_rank_select<terark_entity<rank_select_se_512_64>, false>("se_512_64"     , count, 8ULL * 1024 * 128);
    test_rank_select<terark_entity<rank_select_se_512_64>, true >("se_512_64_fast", count, 8ULL * 1024 * 128);
    test_rank_select<terark_entity<rank_select_se_256_32>, false>("se_256_32"     , count, 8ULL * 1024 * 128);
    test_rank_select<terark_entity<rank_select_se_256_32>, true >("se_256_32_fast", count, 8ULL * 1024 * 128);
    test_rank_select<terark_entity<rank_select_il_256_32>, false>("il_256_32"     , count, 8ULL * 1024 * 128);
    test_rank_select<terark_entity<rank_select_il_256_32>, true >("il_256_32_fast", count, 8ULL * 1024 * 128);
    fprintf(stderr, "\n");
    test_rank_select<  sdsl_entity                       , false>("sdsl_v_mcl"    , count, 8ULL * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_512_32>, false>("se_512_32"     , count, 8ULL * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_512_32>, true >("se_512_32_fast", count, 8ULL * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_512_64>, false>("se_512_64"     , count, 8ULL * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_512_64>, true >("se_512_64_fast", count, 8ULL * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_256_32>, false>("se_256_32"     , count, 8ULL * 1024 * 4);
    test_rank_select<terark_entity<rank_select_se_256_32>, true >("se_256_32_fast", count, 8ULL * 1024 * 4);
    test_rank_select<terark_entity<rank_select_il_256_32>, false>("il_256_32"     , count, 8ULL * 1024 * 4);
    test_rank_select<terark_entity<rank_select_il_256_32>, true >("il_256_32_fast", count, 8ULL * 1024 * 4);

    //system("pause");
    return 0;
}

