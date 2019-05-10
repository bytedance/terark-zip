#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#define _SCL_SECURE_NO_WARNINGS
#else
#endif
#include <stdio.h>
#include <terark/bitmap.hpp>
#include <terark/rank_select.hpp>
#include <chrono>

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

using namespace terark;

template<class T, bool FAST, bool UNROLL = false>
struct test_rank_select {
  size_t sum = 0;

  test_rank_select(std::string name, size_t count, size_t size) {
//    ro = std::chrono::seconds(0);
//    so = std::chrono::seconds(0);
//    rr = std::chrono::seconds(0);
//    sr = std::chrono::seconds(0);
//    osqr = std::chrono::seconds(0);
//    osqrr = std::chrono::seconds(0);
//    zsqr = std::chrono::seconds(0);
//    zsqrr = std::chrono::seconds(0);
    std::mt19937_64 mt(0);
    size_t final_count = 0;
    double total_size = 0;
    for (size_t i = 0; i < 8; ++i) {
      size_t sub_count = (count + 7) / 8 + size_t(mt() & 64ULL);
      total_size += test(sub_count, size, mt, sum, std::integral_constant<bool, FAST>());
      final_count += sub_count;
    }
    total_size /= 8;
//    fprintf(stderr,
//            "%16s\t"
//            "%8.3f\t"   //rank_ordered
//            "%8.3f\t"   //select_ordered
//            "%8.3f\t"   //rank_random
//            "%8.3f\t"   //select_random
//            "%8.3f\t"   //one_seq_len
//            "%8.3f\t"   //zero_seq_len
//            "%12zd\t"   //bit_count
//            "%12zd\t"   //bit_size
//            "%12.1f\t"  //total_size
//            "%8.3f\t"   //expand_rate
//            "%016zX\n"  //check_sum
//            , name.c_str(),
//            ro.count() / final_count,
//            so.count() / final_count,
//            rr.count() / final_count,
//            sr.count() / final_count,
//            osqr.count() / final_count,
//            zsqr.count() / final_count,
//            size,
//            (size + 7) / 8,
//            total_size,
//            total_size / ((size + 7) / 8),
//            sum
//    );
  }

  size_t retSum(){
    return sum;
  }

  //static auto
  void do_test(std::function<size_t()> f) {
    //auto begin = std::chrono::high_resolution_clock::now();
    size_t sum = f();
    TERARK_UNUSED_VAR(sum);
    //auto end = std::chrono::high_resolution_clock::now();
    //return std::chrono::duration_cast<std::chrono::duration<double, std::nano>>(end - begin);
  }

  size_t test(size_t count, size_t size, std::mt19937_64& mt, size_t &sum, std::false_type) {
    std::vector<size_t> r;
    r.resize(count);
    T t;
    t.init(mt, size);
    size_t max_rank = t.max_rank();
    if (max_rank == 0)
      throw 0;

    //ro +=
    do_test([&, count, size] {
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

    //so +=
    do_test([&, count, max_rank] {
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
    //rr +=
    do_test([&, count] {
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
    //sr +=
    do_test([&, count] {
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

    std::generate(r.begin(), r.end(), std::bind(std::uniform_int_distribution<size_t>(0, max_rank - 1), std::ref(mt)));
    //osqr +=
    do_test([&, count] {
      size_t s = 0;
      size_t i = 0;
      if (UNROLL) {
        for(; i + 3 < count; i += 4)
          s += t.one_seq_len(r[i]) + t.one_seq_len(r[i + 1]) + t.one_seq_len(r[i + 2]) + t.one_seq_len(r[i + 3]);
      }
      for(; i < count; ++i)
        s += t.one_seq_len(r[i]);
      return sum += s;
    });

    std::generate(r.begin(), r.end(), std::bind(std::uniform_int_distribution<size_t>(0, max_rank - 1), std::ref(mt)));
    //osqrr +=
    do_test([&, count] {
      size_t s = 0;
      size_t i = 0;
      if (UNROLL) {
        for(; i + 3 < count; i += 4)
          s += t.one_seq_revlen(r[i]) + t.one_seq_revlen(r[i + 1]) + t.one_seq_revlen(r[i + 2]) + t.one_seq_revlen(r[i + 3]);
      }
      for(; i < count; ++i)
        s += t.one_seq_revlen(r[i]);
      return sum += s;
    });

    std::generate(r.begin(), r.end(), std::bind(std::uniform_int_distribution<size_t>(0, max_rank - 1), std::ref(mt)));
    //zsqr +=
    do_test([&, count] {
      size_t s = 0;
      size_t i = 0;
      if (UNROLL) {
        for(; i + 3 < count; i += 4)
          s += t.zero_seq_len(r[i]) + t.zero_seq_len(r[i + 1]) + t.zero_seq_len(r[i + 2]) + t.zero_seq_len(r[i + 3]);
      }
      for(; i < count; ++i)
        s += t.zero_seq_len(r[i]);
      return sum += s;
    });
    std::generate(r.begin(), r.end(), std::bind(std::uniform_int_distribution<size_t>(0, max_rank - 1), std::ref(mt)));
    //zsqrr +=
    do_test([&, count] {
      size_t s = 0;
      size_t i = 0;
      if (UNROLL) {
        for(; i + 3 < count; i += 4)
          s += t.zero_seq_revlen(r[i]) + t.zero_seq_revlen(r[i + 1]) + t.zero_seq_revlen(r[i + 2]) + t.zero_seq_revlen(r[i + 3]);
      }
      for(; i < count; ++i)
        s += t.zero_seq_revlen(r[i]);
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

    //ro +=
    do_test([&, count, size] {
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
    //so +=
    do_test([&, count, max_rank] {
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
    //rr +=
    do_test([&, count] {
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
    //sr +=
    do_test([&, count] {
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
//  std::chrono::duration<double, std::nano> ro;
//  std::chrono::duration<double, std::nano> so;
//  std::chrono::duration<double, std::nano> rr;
//  std::chrono::duration<double, std::nano> sr;
//  std::chrono::duration<double, std::nano> osqr;
//  std::chrono::duration<double, std::nano> osqrr;
//  std::chrono::duration<double, std::nano> zsqr;
//  std::chrono::duration<double, std::nano> zsqrr;
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

  size_t one_seq_len(size_t i) const {
    return rs.one_seq_len(i);
  }

  size_t zero_seq_len(size_t i) const {
    return rs.zero_seq_len(i);
  }

  size_t zero_seq_revlen(size_t i) const {
    return rs.zero_seq_revlen(i);
  }
  size_t one_seq_revlen(size_t i) const { return rs.one_seq_revlen(i);}
};

template<size_t P, size_t W>
struct terark_few {
  rank_select_se_512_64 rs_build;
  terark::rank_select_few<P, W> rs;
  size_t hint;

  void init(std::mt19937_64& mt, size_t size) {
    //rank_select_se_512_64 rs_build;
    rs_build.resize(size);
    for (size_t i = 0, max = (size + 63) / 64; i < max; ++i) {
      rs_build.set_word(i, mt());
      if (rs_build.get_word(i) % 4 == 0)
        rs_build.set_word(i, size_t(-1));
      if (mt() % 5 == 1)
        rs_build.set_word(i, size_t(0));
    }
    rs_build.build_cache(false, false);
    rank_select_few_builder<P, W> b(rs_build.max_rank0(), rs_build.max_rank1(), false);
    for (size_t i = 0; i < rs_build.size(); ++i) {
      if (rs_build[i])
        b.insert(i);
    }
    b.finish(&rs);
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

  inline size_t select(size_t i) const {
    return rs.select1(i);
  }

  size_t one_seq_len(size_t i) const {
    return rs.one_seq_len(i);
  }

  size_t one_seq_revlen(size_t i) const {
    return rs.one_seq_revlen(i);
  }

  size_t zero_seq_len(size_t i) const {
    return rs.zero_seq_len(i);
  }

  size_t zero_seq_revlen(size_t i) const {
    return rs.zero_seq_revlen(i);
  }
};

int main(int argc, char* argv[]) {
//  fprintf(stderr,
//          "name\t"
//          "rank_ordered\t"
//          "select_ordered\t"
//          "rank_random\t"
//          "select_random\t"
//          "one_seq_len\t"
//          "zero_seq_len\t"
//          "bit_count\t"
//          "bit_bytes\t"
//          "total_bytes\t"
//          "expand_ratio\t"
//          "check_sum\n");
//  fprintf(stderr, "\n");

  size_t count = 100000, maxMB = 512;
  if (argc > 1)
    count = (size_t)strtoull(argv[1], NULL, 10);
  if (argc > 2)
    maxMB = (size_t)strtoull(argv[2], NULL, 10);

  size_t check;
  check = test_rank_select<terark_entity<rank_select_se_512_32>, false>("se_512_32"     , count, 8ULL * 1024 * 4).retSum();
  if(check != test_rank_select<terark_entity<rank_select_se_512_64>, false>("se_512_64"     , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_se_256_32>, false>("se_256_32"     , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_il_256_32>, false>("il_256_32"     , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 4>                    , false>("few0_4"        , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 5>                    , false>("few0_5"        , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 6>                    , false>("few0_6"        , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 7>                    , false>("few0_7"        , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 8>                    , false>("few0_8"        , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 4>                    , false>("few1_4"        , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 5>                    , false>("few1_5"        , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 6>                    , false>("few1_6"        , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 7>                    , false>("few1_7"        , count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 8>                    , false>("few1_8"        , count, 8ULL * 1024 * 4).retSum()) return -1;
  //fprintf(stderr, "\n");
  check = test_rank_select<terark_entity<rank_select_se_512_32>, true >("se_512_32_fast", count, 8ULL * 1024 * 4).retSum();
  if(check != test_rank_select<terark_entity<rank_select_se_512_64>, true >("se_512_64_fast", count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_se_256_32>, true >("se_256_32_fast", count, 8ULL * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_il_256_32>, true >("il_256_32_fast", count, 8ULL * 1024 * 4).retSum()) return -1;

  //fprintf(stderr, "\n");
  check = test_rank_select<terark_entity<rank_select_se_512_32>, false>("se_512_32"     , count, 8ULL * 1024 * 128).retSum();
  if(check != test_rank_select<terark_entity<rank_select_se_512_64>, false>("se_512_64"     , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_se_256_32>, false>("se_256_32"     , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_il_256_32>, false>("il_256_32"     , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 4>                    , false>("few0_4"        , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 5>                    , false>("few0_5"        , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 6>                    , false>("few0_6"        , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 7>                    , false>("few0_7"        , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 8>                    , false>("few0_8"        , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 4>                    , false>("few1_4"        , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 5>                    , false>("few1_5"        , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 6>                    , false>("few1_6"        , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 7>                    , false>("few1_7"        , count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 8>                    , false>("few1_8"        , count, 8ULL * 1024 * 128).retSum()) return -1;
  //fprintf(stderr, "\n");
  check = test_rank_select<terark_entity<rank_select_se_512_32>, true >("se_512_32_fast", count, 8ULL * 1024 * 128).retSum();
  if(check != test_rank_select<terark_entity<rank_select_se_512_64>, true >("se_512_64_fast", count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_se_256_32>, true >("se_256_32_fast", count, 8ULL * 1024 * 128).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_il_256_32>, true >("il_256_32_fast", count, 8ULL * 1024 * 128).retSum()) return -1;
/*
  //fprintf(stderr, "\n");
  check = test_rank_select<terark_entity<rank_select_se_512_32>, false>("se_512_32"     , count, 8ULL * 1024 * 1024 * 4).retSum();
  if(check != test_rank_select<terark_entity<rank_select_se_512_64>, false>("se_512_64"     , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_se_256_32>, false>("se_256_32"     , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_il_256_32>, false>("il_256_32"     , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 4>                    , false>("few0_4"        , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 5>                    , false>("few0_5"        , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 6>                    , false>("few0_6"        , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 7>                    , false>("few0_7"        , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 8>                    , false>("few0_8"        , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 4>                    , false>("few1_4"        , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 5>                    , false>("few1_5"        , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 6>                    , false>("few1_6"        , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 7>                    , false>("few1_7"        , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 8>                    , false>("few1_8"        , count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  //fprintf(stderr, "\n");
  check = test_rank_select<terark_entity<rank_select_se_512_32>, true >("se_512_32_fast", count, 8ULL * 1024 * 1024 * 4).retSum();
  if(check != test_rank_select<terark_entity<rank_select_se_512_64>, true >("se_512_64_fast", count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_se_256_32>, true >("se_256_32_fast", count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_il_256_32>, true >("il_256_32_fast", count, 8ULL * 1024 * 1024 * 4).retSum()) return -1;

if (maxMB > 4) {
  //fprintf(stderr, "\n");
  check = test_rank_select<terark_entity<rank_select_se_512_32>, false>("se_512_32"     , count, 8ULL * 1024 * 1024 * maxMB).retSum();
  if(check != test_rank_select<terark_entity<rank_select_se_512_64>, false>("se_512_64"     , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_se_256_32>, false>("se_256_32"     , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_il_256_32>, false>("il_256_32"     , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 4>                    , false>("few0_4"        , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 5>                    , false>("few0_5"        , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 6>                    , false>("few0_6"        , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 7>                    , false>("few0_7"        , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_few<0, 8>                    , false>("few0_8"        , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 4>                    , false>("few1_4"        , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 5>                    , false>("few1_5"        , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 6>                    , false>("few1_6"        , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 7>                    , false>("few1_7"        , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_few<1, 8>                    , false>("few1_8"        , count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  //fprintf(stderr, "\n");
  check = test_rank_select<terark_entity<rank_select_se_512_32>, true >("se_512_32_fast", count, 8ULL * 1024 * 1024 * maxMB).retSum();
  if(check != test_rank_select<terark_entity<rank_select_se_512_64>, true >("se_512_64_fast", count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_se_256_32>, true >("se_256_32_fast", count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  if(check != test_rank_select<terark_entity<rank_select_il_256_32>, true >("il_256_32_fast", count, 8ULL * 1024 * 1024 * maxMB).retSum()) return -1;
  //fprintf(stderr, "\n");
}
*/
  fprintf(stderr, "rank select unit tests all passed!\n");
  return 0;
}
