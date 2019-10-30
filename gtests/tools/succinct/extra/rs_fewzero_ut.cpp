
#include <memory>

#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <terark/io/FileStream.hpp>
#include <terark/rank_select.hpp>

//#include "../terark_zip_common.h"
//#include "../terark_zip_index.h"

using std::string;
using std::unique_ptr;

using terark::byte_t;
using terark::fstring;
using terark::valvec;
using terark::valvec_no_init;
using terark::valvec_reserve;

using terark::FileStream;
using terark::InputBuffer;
using terark::OutputBuffer;
using terark::LittleEndianDataInput;
using terark::LittleEndianDataOutput;

/*
 * pos: 0 1 2 3 4 5 6
 *      1 0 1 1 0 0 1
 */
void test_fewzero() {
  printf("==== Test fewzero @ last is '1'\n");
  const size_t kCnt = 7;
  //terark::rank_select_simple simple(kCnt);
  terark::rank_select_il_256_32 simple(kCnt);
  size_t pos = 0;
  simple.set1(pos++); simple.set0(pos++);
  simple.set1(pos++); simple.set1(pos++);
  simple.set0(pos++); simple.set0(pos++);
  simple.set1(pos++);
  simple.build_cache(true, true);

  rank_select_fewzero<uint32_t> rs(kCnt);
  rs.build_from(simple);

  {
    size_t memsz = rs.mem_size();
    assert(memsz % 8 == 0);
  }
  {
    size_t hint;
    assert(rs.zero_seq_len(0, hint) == 0);
    assert(rs.zero_seq_len(0, hint) == simple.zero_seq_len(0));
    assert(rs.zero_seq_len(1, hint) == 1);
    assert(rs.zero_seq_len(1, hint) == simple.zero_seq_len(1));
    assert(rs.zero_seq_len(2, hint) == 0);
    assert(rs.zero_seq_len(2, hint) == simple.zero_seq_len(2));
    assert(rs.zero_seq_len(3, hint) == 0);
    assert(rs.zero_seq_len(3, hint) == simple.zero_seq_len(3));
    assert(rs.zero_seq_len(4, hint) == 2);
    assert(rs.zero_seq_len(4, hint) == simple.zero_seq_len(4));
    assert(rs.zero_seq_len(5, hint) == 1);
    assert(rs.zero_seq_len(5, hint) == simple.zero_seq_len(5));
    assert(rs.zero_seq_len(6, hint) == 0);
    assert(rs.zero_seq_len(6, hint) == simple.zero_seq_len(6));
  }
  {
    size_t hint;
    assert(rs.zero_seq_revlen(0, hint) == 0);
    assert(rs.zero_seq_revlen(0, hint) == simple.zero_seq_revlen(0));
    assert(rs.zero_seq_revlen(1, hint) == 0);
    assert(rs.zero_seq_revlen(1, hint) == simple.zero_seq_revlen(1));
    assert(rs.zero_seq_revlen(2, hint) == 1);
    assert(rs.zero_seq_revlen(2, hint) == simple.zero_seq_revlen(2));
    assert(rs.zero_seq_revlen(3, hint) == 0);
    assert(rs.zero_seq_revlen(3, hint) == simple.zero_seq_revlen(3));
    assert(rs.zero_seq_revlen(4, hint) == 0);
    assert(rs.zero_seq_revlen(4, hint) == simple.zero_seq_revlen(4));
    assert(rs.zero_seq_revlen(5, hint) == 1);
    assert(rs.zero_seq_revlen(5, hint) == simple.zero_seq_revlen(5));
    assert(rs.zero_seq_revlen(6, hint) == 2);
    assert(rs.zero_seq_revlen(6, hint) == simple.zero_seq_revlen(6));
  }
  {
    assert(rs.zero_seq_revlen(0) == 0);
    assert(rs.zero_seq_revlen(0) == simple.zero_seq_revlen(0));
    assert(rs.zero_seq_revlen(1) == 0);
    assert(rs.zero_seq_revlen(1) == simple.zero_seq_revlen(1));
    assert(rs.zero_seq_revlen(2) == 1);
    assert(rs.zero_seq_revlen(2) == simple.zero_seq_revlen(2));
    assert(rs.zero_seq_revlen(3) == 0);
    assert(rs.zero_seq_revlen(3) == simple.zero_seq_revlen(3));
    assert(rs.zero_seq_revlen(4) == 0);
    assert(rs.zero_seq_revlen(4) == simple.zero_seq_revlen(4));
    assert(rs.zero_seq_revlen(5) == 1);
    assert(rs.zero_seq_revlen(5) == simple.zero_seq_revlen(5));
    assert(rs.zero_seq_revlen(6) == 2);
    assert(rs.zero_seq_revlen(6) == simple.zero_seq_revlen(6));
  }
  {
    assert(rs.one_seq_revlen(0) == 0);
    assert(rs.one_seq_revlen(0) == simple.one_seq_revlen(0));
    assert(rs.one_seq_revlen(1) == 1);
    assert(rs.one_seq_revlen(1) == simple.one_seq_revlen(1));
    assert(rs.one_seq_revlen(2) == 0);
    assert(rs.one_seq_revlen(2) == simple.one_seq_revlen(2));
    assert(rs.one_seq_revlen(3) == 1);
    assert(rs.one_seq_revlen(3) == simple.one_seq_revlen(3));
    assert(rs.one_seq_revlen(4) == 2);
    assert(rs.one_seq_revlen(4) == simple.one_seq_revlen(4));
    assert(rs.one_seq_revlen(5) == 0);
    assert(rs.one_seq_revlen(5) == simple.one_seq_revlen(5));
    assert(rs.one_seq_revlen(6) == 0);
    assert(rs.one_seq_revlen(6) == simple.one_seq_revlen(6));
  }
  {
    assert(rs.zero_seq_len(0) == 0);
    assert(rs.zero_seq_len(0) == simple.zero_seq_len(0));
    assert(rs.zero_seq_len(1) == 1);
    assert(rs.zero_seq_len(1) == simple.zero_seq_len(1));
    assert(rs.zero_seq_len(2) == 0);
    assert(rs.zero_seq_len(2) == simple.zero_seq_len(2));
    assert(rs.zero_seq_len(3) == 0);
    assert(rs.zero_seq_len(3) == simple.zero_seq_len(3));
    assert(rs.zero_seq_len(4) == 2);
    assert(rs.zero_seq_len(4) == simple.zero_seq_len(4));
    assert(rs.zero_seq_len(5) == 1);
    assert(rs.zero_seq_len(5) == simple.zero_seq_len(5));
    assert(rs.zero_seq_len(6) == 0);
    assert(rs.zero_seq_len(6) == simple.zero_seq_len(6));
  }
  {
    assert(rs.one_seq_len(0) == 1);
    assert(rs.one_seq_len(0) == simple.one_seq_len(0));
    assert(rs.one_seq_len(1) == 0);
    assert(rs.one_seq_len(1) == simple.one_seq_len(1));
    assert(rs.one_seq_len(2) == 2);
    assert(rs.one_seq_len(2) == simple.one_seq_len(2));
    assert(rs.one_seq_len(3) == 1);
    assert(rs.one_seq_len(3) == simple.one_seq_len(3));
    assert(rs.one_seq_len(4) == 0);
    assert(rs.one_seq_len(4) == simple.one_seq_len(4));
    assert(rs.one_seq_len(5) == 0);
    assert(rs.one_seq_len(5) == simple.one_seq_len(5));
    assert(rs.one_seq_len(6) == 1);
    assert(rs.one_seq_len(6) == simple.one_seq_len(6));
  }
  {
    assert(rs.is0(0) == false);
    assert(rs.is0(0) == simple.is0(0));
    assert(rs.is0(1) == true);
    assert(rs.is0(1) == simple.is0(1));
    assert(rs.is0(2) == false);
    assert(rs.is0(2) == simple.is0(2));
    assert(rs.is0(3) == false);
    assert(rs.is0(3) == simple.is0(3));
    assert(rs.is0(4) == true);
    assert(rs.is0(4) == simple.is0(4));
    assert(rs.is0(5) == true);
    assert(rs.is0(5) == simple.is0(5));
    assert(rs.is0(6) == false);
    assert(rs.is0(6) == simple.is0(6));
  }
  {
    assert(rs.is1(0) == true);
    assert(rs.is1(0) == simple.is1(0));
    assert(rs.is1(1) == false);
    assert(rs.is1(1) == simple.is1(1));
    assert(rs.is1(2) == true);
    assert(rs.is1(2) == simple.is1(2));
    assert(rs.is1(3) == true);
    assert(rs.is1(3) == simple.is1(3));
    assert(rs.is1(4) == false);
    assert(rs.is1(4) == simple.is1(4));
    assert(rs.is1(5) == false);
    assert(rs.is1(5) == simple.is1(5));
    assert(rs.is1(6) == true);
    assert(rs.is1(6) == simple.is1(6));
  }
  {
    assert(rs.rank0(0) == 0);
    assert(rs.rank0(0) == simple.rank0(0));
    assert(rs.rank0(1) == 0);
    assert(rs.rank0(1) == simple.rank0(1));
    assert(rs.rank0(2) == 1);
    assert(rs.rank0(2) == simple.rank0(2));
    assert(rs.rank0(3) == 1);
    assert(rs.rank0(3) == simple.rank0(3));
    assert(rs.rank0(4) == 1);
    assert(rs.rank0(4) == simple.rank0(4));
    assert(rs.rank0(5) == 2);
    assert(rs.rank0(5) == simple.rank0(5));
    assert(rs.rank0(6) == 3);
    assert(rs.rank0(6) == simple.rank0(6));
  }
  {
    assert(rs.rank1(0) == 0);
    assert(rs.rank1(0) == simple.rank1(0));
    assert(rs.rank1(1) == 1);
    assert(rs.rank1(1) == simple.rank1(1));
    assert(rs.rank1(2) == 1);
    assert(rs.rank1(2) == simple.rank1(2));
    assert(rs.rank1(3) == 2);
    assert(rs.rank1(3) == simple.rank1(3));
    assert(rs.rank1(4) == 3);
    assert(rs.rank1(4) == simple.rank1(4));
    assert(rs.rank1(5) == 3);
    assert(rs.rank1(5) == simple.rank1(5));
    assert(rs.rank1(6) == 3);
    assert(rs.rank1(6) == simple.rank1(6));
  }
  {
    assert(rs.select0(0) == 1);
    assert(rs.select0(0) == simple.select0(0));
    assert(rs.select0(1) == 4);
    assert(rs.select0(1) == simple.select0(1));
    assert(rs.select0(2) == 5);
    assert(rs.select0(2) == simple.select0(2));
  }
  {
    assert(rs.select1(0) == 0);
    assert(rs.select1(0) == simple.select1(0));
    assert(rs.select1(1) == 2);
    assert(rs.select1(1) == simple.select1(1));
    assert(rs.select1(2) == 3);
    assert(rs.select1(2) == simple.select1(2));
    assert(rs.select1(3) == 6);
    assert(rs.select1(3) == simple.select1(3));
  }
  {
    assert(rs.max_rank0() == 3);
    assert(rs.max_rank0() == simple.max_rank0());
    assert(rs.max_rank1() == 4);
    assert(rs.max_rank1() == simple.max_rank1());
  }

  printf("\tdone\n");
}

/*
 * pos: 0 1 2 3 4 5 6
 *      1 0 1 1 0 0 1
 */
void test_fewone() {
  printf("==== Test fewone @ last is '1'\n");
  const size_t kCnt = 7;
  //terark::rank_select_simple simple(kCnt);
  terark::rank_select_il_256_32 simple(kCnt);
  size_t pos = 0;
  simple.set1(pos++); simple.set0(pos++);
  simple.set1(pos++); simple.set1(pos++);
  simple.set0(pos++); simple.set0(pos++);
  simple.set1(pos++);
  simple.build_cache(true, true);

  rank_select_fewone<uint32_t> rs(kCnt);
  rs.build_from(simple);
  
  {
    size_t memsz = rs.mem_size();
    assert(memsz % 8 == 0);
  }
  {
    size_t hint;
    assert(rs.zero_seq_len(0, hint) == 0);
    assert(rs.zero_seq_len(0, hint) == simple.zero_seq_len(0));
    assert(rs.zero_seq_len(1, hint) == 1);
    assert(rs.zero_seq_len(1, hint) == simple.zero_seq_len(1));
    assert(rs.zero_seq_len(2, hint) == 0);
    assert(rs.zero_seq_len(2, hint) == simple.zero_seq_len(2));
    assert(rs.zero_seq_len(3, hint) == 0);
    assert(rs.zero_seq_len(3, hint) == simple.zero_seq_len(3));
    assert(rs.zero_seq_len(4, hint) == 2);
    assert(rs.zero_seq_len(4, hint) == simple.zero_seq_len(4));
    assert(rs.zero_seq_len(5, hint) == 1);
    assert(rs.zero_seq_len(5, hint) == simple.zero_seq_len(5));
    assert(rs.zero_seq_len(6, hint) == 0);
    assert(rs.zero_seq_len(6, hint) == simple.zero_seq_len(6));
  }
  {
    size_t hint;
    assert(rs.zero_seq_revlen(0, hint) == 0);
    assert(rs.zero_seq_revlen(0, hint) == simple.zero_seq_revlen(0));
    assert(rs.zero_seq_revlen(1, hint) == 0);
    assert(rs.zero_seq_revlen(1, hint) == simple.zero_seq_revlen(1));
    assert(rs.zero_seq_revlen(2, hint) == 1);
    assert(rs.zero_seq_revlen(2, hint) == simple.zero_seq_revlen(2));
    assert(rs.zero_seq_revlen(3, hint) == 0);
    assert(rs.zero_seq_revlen(3, hint) == simple.zero_seq_revlen(3));
    assert(rs.zero_seq_revlen(4, hint) == 0);
    assert(rs.zero_seq_revlen(4, hint) == simple.zero_seq_revlen(4));
    assert(rs.zero_seq_revlen(5, hint) == 1);
    assert(rs.zero_seq_revlen(5, hint) == simple.zero_seq_revlen(5));
    assert(rs.zero_seq_revlen(6, hint) == 2);
    assert(rs.zero_seq_revlen(6, hint) == simple.zero_seq_revlen(6));
  }

  {
    assert(rs.zero_seq_revlen(0) == 0);
    assert(rs.zero_seq_revlen(0) == simple.zero_seq_revlen(0));
    assert(rs.zero_seq_revlen(1) == 0);
    assert(rs.zero_seq_revlen(1) == simple.zero_seq_revlen(1));
    assert(rs.zero_seq_revlen(2) == 1);
    assert(rs.zero_seq_revlen(2) == simple.zero_seq_revlen(2));
    assert(rs.zero_seq_revlen(3) == 0);
    assert(rs.zero_seq_revlen(3) == simple.zero_seq_revlen(3));
    assert(rs.zero_seq_revlen(4) == 0);
    assert(rs.zero_seq_revlen(4) == simple.zero_seq_revlen(4));
    assert(rs.zero_seq_revlen(5) == 1);
    assert(rs.zero_seq_revlen(5) == simple.zero_seq_revlen(5));
    assert(rs.zero_seq_revlen(6) == 2);
    assert(rs.zero_seq_revlen(6) == simple.zero_seq_revlen(6));
  }
  {
    assert(rs.one_seq_revlen(0) == 0);
    assert(rs.one_seq_revlen(0) == simple.one_seq_revlen(0));
    assert(rs.one_seq_revlen(1) == 1);
    assert(rs.one_seq_revlen(1) == simple.one_seq_revlen(1));
    assert(rs.one_seq_revlen(2) == 0);
    assert(rs.one_seq_revlen(2) == simple.one_seq_revlen(2));
    assert(rs.one_seq_revlen(3) == 1);
    assert(rs.one_seq_revlen(3) == simple.one_seq_revlen(3));
    assert(rs.one_seq_revlen(4) == 2);
    assert(rs.one_seq_revlen(4) == simple.one_seq_revlen(4));
    assert(rs.one_seq_revlen(5) == 0);
    assert(rs.one_seq_revlen(5) == simple.one_seq_revlen(5));
    assert(rs.one_seq_revlen(6) == 0);
    assert(rs.one_seq_revlen(6) == simple.one_seq_revlen(6));
  }
  {
    assert(rs.zero_seq_len(0) == 0);
    assert(rs.zero_seq_len(0) == simple.zero_seq_len(0));
    assert(rs.zero_seq_len(1) == 1);
    assert(rs.zero_seq_len(1) == simple.zero_seq_len(1));
    assert(rs.zero_seq_len(2) == 0);
    assert(rs.zero_seq_len(2) == simple.zero_seq_len(2));
    assert(rs.zero_seq_len(3) == 0);
    assert(rs.zero_seq_len(3) == simple.zero_seq_len(3));
    assert(rs.zero_seq_len(4) == 2);
    assert(rs.zero_seq_len(4) == simple.zero_seq_len(4));
    assert(rs.zero_seq_len(5) == 1);
    assert(rs.zero_seq_len(5) == simple.zero_seq_len(5));
    assert(rs.zero_seq_len(6) == 0);
    assert(rs.zero_seq_len(6) == simple.zero_seq_len(6));
  }
  {
    assert(rs.one_seq_len(0) == 1);
    assert(rs.one_seq_len(0) == simple.one_seq_len(0));
    assert(rs.one_seq_len(1) == 0);
    assert(rs.one_seq_len(1) == simple.one_seq_len(1));
    assert(rs.one_seq_len(2) == 2);
    assert(rs.one_seq_len(2) == simple.one_seq_len(2));
    assert(rs.one_seq_len(3) == 1);
    assert(rs.one_seq_len(3) == simple.one_seq_len(3));
    assert(rs.one_seq_len(4) == 0);
    assert(rs.one_seq_len(4) == simple.one_seq_len(4));
    assert(rs.one_seq_len(5) == 0);
    assert(rs.one_seq_len(5) == simple.one_seq_len(5));
    assert(rs.one_seq_len(6) == 1);
    assert(rs.one_seq_len(6) == simple.one_seq_len(6));
  }
  {
    assert(rs.is0(0) == false);
    assert(rs.is0(0) == simple.is0(0));
    assert(rs.is0(1) == true);
    assert(rs.is0(1) == simple.is0(1));
    assert(rs.is0(2) == false);
    assert(rs.is0(2) == simple.is0(2));
    assert(rs.is0(3) == false);
    assert(rs.is0(3) == simple.is0(3));
    assert(rs.is0(4) == true);
    assert(rs.is0(4) == simple.is0(4));
    assert(rs.is0(5) == true);
    assert(rs.is0(5) == simple.is0(5));
    assert(rs.is0(6) == false);
    assert(rs.is0(6) == simple.is0(6));
  }
  {
    assert(rs.is1(0) == true);
    assert(rs.is1(0) == simple.is1(0));
    assert(rs.is1(1) == false);
    assert(rs.is1(1) == simple.is1(1));
    assert(rs.is1(2) == true);
    assert(rs.is1(2) == simple.is1(2));
    assert(rs.is1(3) == true);
    assert(rs.is1(3) == simple.is1(3));
    assert(rs.is1(4) == false);
    assert(rs.is1(4) == simple.is1(4));
    assert(rs.is1(5) == false);
    assert(rs.is1(5) == simple.is1(5));
    assert(rs.is1(6) == true);
    assert(rs.is1(6) == simple.is1(6));
  }
  {
    assert(rs.rank0(0) == 0);
    assert(rs.rank0(0) == simple.rank0(0));
    assert(rs.rank0(1) == 0);
    assert(rs.rank0(1) == simple.rank0(1));
    assert(rs.rank0(2) == 1);
    assert(rs.rank0(2) == simple.rank0(2));
    assert(rs.rank0(3) == 1);
    assert(rs.rank0(3) == simple.rank0(3));
    assert(rs.rank0(4) == 1);
    assert(rs.rank0(4) == simple.rank0(4));
    assert(rs.rank0(5) == 2);
    assert(rs.rank0(5) == simple.rank0(5));
    assert(rs.rank0(6) == 3);
    assert(rs.rank0(6) == simple.rank0(6));
  }
  {
    assert(rs.rank1(0) == 0);
    assert(rs.rank1(0) == simple.rank1(0));
    assert(rs.rank1(1) == 1);
    assert(rs.rank1(1) == simple.rank1(1));
    assert(rs.rank1(2) == 1);
    assert(rs.rank1(2) == simple.rank1(2));
    assert(rs.rank1(3) == 2);
    assert(rs.rank1(3) == simple.rank1(3));
    assert(rs.rank1(4) == 3);
    assert(rs.rank1(4) == simple.rank1(4));
    assert(rs.rank1(5) == 3);
    assert(rs.rank1(5) == simple.rank1(5));
    assert(rs.rank1(6) == 3);
    assert(rs.rank1(6) == simple.rank1(6));
  }
  {
    assert(rs.select0(0) == 1);
    assert(rs.select0(0) == simple.select0(0));
    assert(rs.select0(1) == 4);
    assert(rs.select0(1) == simple.select0(1));
    assert(rs.select0(2) == 5);
    assert(rs.select0(2) == simple.select0(2));
  }
  {
    assert(rs.select1(0) == 0);
    assert(rs.select1(0) == simple.select1(0));
    assert(rs.select1(1) == 2);
    assert(rs.select1(1) == simple.select1(1));
    assert(rs.select1(2) == 3);
    assert(rs.select1(2) == simple.select1(2));
    assert(rs.select1(3) == 6);
    assert(rs.select1(3) == simple.select1(3));
  }
  {
    assert(rs.max_rank0() == 3);
    assert(rs.max_rank0() == simple.max_rank0());
    assert(rs.max_rank1() == 4);
    assert(rs.max_rank1() == simple.max_rank1());
  }

  printf("\tdone\n");
}


/////////////////

/*
 * pos: 0 1 2 3 4 5
 *      1 0 1 1 0 0
 */
void test_fewzero_risk() {
  printf("==== Test fewzero @ last is '0' && risk mmap data\n");
  const size_t kCnt = 6;
  //terark::rank_select_simple simple(kCnt);
  terark::rank_select_il_256_32 simple(kCnt);
  size_t pos = 0;
  simple.set1(pos++); simple.set0(pos++);
  simple.set1(pos++); simple.set1(pos++);
  simple.set0(pos++); simple.set0(pos++);
  simple.build_cache(true, true);
  
  rank_select_fewzero<uint32_t> rs_a(kCnt);
  rs_a.build_from(simple);

  size_t memsz = rs_a.mem_size();
  assert(memsz % 8 == 0);
  rank_select_fewzero<uint32_t> rs(kCnt);
  rs.risk_mmap_from((unsigned char*)rs_a.data(), memsz);
  rs_a.risk_release_ownership();
  
  {
    size_t hint;
    assert(rs.zero_seq_len(0, hint) == 0);
    assert(rs.zero_seq_len(0, hint) == simple.zero_seq_len(0));
    assert(rs.zero_seq_len(1, hint) == 1);
    assert(rs.zero_seq_len(1, hint) == simple.zero_seq_len(1));
    assert(rs.zero_seq_len(2, hint) == 0);
    assert(rs.zero_seq_len(2, hint) == simple.zero_seq_len(2));
    assert(rs.zero_seq_len(3, hint) == 0);
    assert(rs.zero_seq_len(3, hint) == simple.zero_seq_len(3));

    assert(rs.zero_seq_len(4, hint) == 2);
    //assert(rs.zero_seq_len(4, hint) == simple.zero_seq_len(4));

    assert(rs.zero_seq_len(5, hint) == 1);
    //assert(rs.zero_seq_len(5, hint) == simple.zero_seq_len(5));
  }
  {
    size_t hint;
    assert(rs.zero_seq_revlen(0, hint) == 0);
    assert(rs.zero_seq_revlen(0, hint) == simple.zero_seq_revlen(0));
    assert(rs.zero_seq_revlen(1, hint) == 0);
    assert(rs.zero_seq_revlen(1, hint) == simple.zero_seq_revlen(1));
    assert(rs.zero_seq_revlen(2, hint) == 1);
    assert(rs.zero_seq_revlen(2, hint) == simple.zero_seq_revlen(2));
    assert(rs.zero_seq_revlen(3, hint) == 0);
    assert(rs.zero_seq_revlen(3, hint) == simple.zero_seq_revlen(3));
    assert(rs.zero_seq_revlen(4, hint) == 0);
    assert(rs.zero_seq_revlen(4, hint) == simple.zero_seq_revlen(4));
    assert(rs.zero_seq_revlen(5, hint) == 1);
    assert(rs.zero_seq_revlen(5, hint) == simple.zero_seq_revlen(5));
  }
  {
    assert(rs.zero_seq_revlen(0) == 0);
    assert(rs.zero_seq_revlen(0) == simple.zero_seq_revlen(0));
    assert(rs.zero_seq_revlen(1) == 0);
    assert(rs.zero_seq_revlen(1) == simple.zero_seq_revlen(1));
    assert(rs.zero_seq_revlen(2) == 1);
    assert(rs.zero_seq_revlen(2) == simple.zero_seq_revlen(2));
    assert(rs.zero_seq_revlen(3) == 0);
    assert(rs.zero_seq_revlen(3) == simple.zero_seq_revlen(3));
    assert(rs.zero_seq_revlen(4) == 0);
    assert(rs.zero_seq_revlen(4) == simple.zero_seq_revlen(4));
    assert(rs.zero_seq_revlen(5) == 1);
    assert(rs.zero_seq_revlen(5) == simple.zero_seq_revlen(5));
  }
  {
    assert(rs.one_seq_revlen(0) == 0);
    assert(rs.one_seq_revlen(0) == simple.one_seq_revlen(0));
    assert(rs.one_seq_revlen(1) == 1);
    assert(rs.one_seq_revlen(1) == simple.one_seq_revlen(1));
    assert(rs.one_seq_revlen(2) == 0);
    assert(rs.one_seq_revlen(2) == simple.one_seq_revlen(2));
    assert(rs.one_seq_revlen(3) == 1);
    assert(rs.one_seq_revlen(3) == simple.one_seq_revlen(3));
    assert(rs.one_seq_revlen(4) == 2);
    assert(rs.one_seq_revlen(4) == simple.one_seq_revlen(4));
    assert(rs.one_seq_revlen(5) == 0);
    assert(rs.one_seq_revlen(5) == simple.one_seq_revlen(5));
  }
  {
    assert(rs.zero_seq_len(0) == 0);
    assert(rs.zero_seq_len(0) == simple.zero_seq_len(0));
    assert(rs.zero_seq_len(1) == 1);
    assert(rs.zero_seq_len(1) == simple.zero_seq_len(1));
    assert(rs.zero_seq_len(2) == 0);
    assert(rs.zero_seq_len(2) == simple.zero_seq_len(2));
    assert(rs.zero_seq_len(3) == 0);
    assert(rs.zero_seq_len(3) == simple.zero_seq_len(3));
    assert(rs.zero_seq_len(4) == 2);
    //assert(rs.zero_seq_len(4) == simple.zero_seq_len(4));
    assert(rs.zero_seq_len(5) == 1);
    //assert(rs.zero_seq_len(5) == simple.zero_seq_len(5));
  }
  {
    assert(rs.one_seq_len(0) == 1);
    assert(rs.one_seq_len(0) == simple.one_seq_len(0));
    assert(rs.one_seq_len(1) == 0);
    assert(rs.one_seq_len(1) == simple.one_seq_len(1));
    assert(rs.one_seq_len(2) == 2);
    assert(rs.one_seq_len(2) == simple.one_seq_len(2));
    assert(rs.one_seq_len(3) == 1);
    assert(rs.one_seq_len(3) == simple.one_seq_len(3));
    assert(rs.one_seq_len(4) == 0);
    assert(rs.one_seq_len(4) == simple.one_seq_len(4));
    assert(rs.one_seq_len(5) == 0);
    assert(rs.one_seq_len(5) == simple.one_seq_len(5));
  }
  {
    assert(rs.is0(0) == false);
    assert(rs.is0(0) == simple.is0(0));
    assert(rs.is0(1) == true);
    assert(rs.is0(1) == simple.is0(1));
    assert(rs.is0(2) == false);
    assert(rs.is0(2) == simple.is0(2));
    assert(rs.is0(3) == false);
    assert(rs.is0(3) == simple.is0(3));
    assert(rs.is0(4) == true);
    assert(rs.is0(4) == simple.is0(4));
    assert(rs.is0(5) == true);
    assert(rs.is0(5) == simple.is0(5));
  }
  {
    assert(rs.is1(0) == true);
    assert(rs.is1(0) == simple.is1(0));
    assert(rs.is1(1) == false);
    assert(rs.is1(1) == simple.is1(1));
    assert(rs.is1(2) == true);
    assert(rs.is1(2) == simple.is1(2));
    assert(rs.is1(3) == true);
    assert(rs.is1(3) == simple.is1(3));
    assert(rs.is1(4) == false);
    assert(rs.is1(4) == simple.is1(4));
    assert(rs.is1(5) == false);
    assert(rs.is1(5) == simple.is1(5));
  }
  {
    assert(rs.rank0(0) == 0);
    assert(rs.rank0(0) == simple.rank0(0));
    assert(rs.rank0(1) == 0);
    assert(rs.rank0(1) == simple.rank0(1));
    assert(rs.rank0(2) == 1);
    assert(rs.rank0(2) == simple.rank0(2));
    assert(rs.rank0(3) == 1);
    assert(rs.rank0(3) == simple.rank0(3));
    assert(rs.rank0(4) == 1);
    assert(rs.rank0(4) == simple.rank0(4));
    assert(rs.rank0(5) == 2);
    assert(rs.rank0(5) == simple.rank0(5));
  }
  {
    assert(rs.rank1(0) == 0);
    assert(rs.rank1(0) == simple.rank1(0));
    assert(rs.rank1(1) == 1);
    assert(rs.rank1(1) == simple.rank1(1));
    assert(rs.rank1(2) == 1);
    assert(rs.rank1(2) == simple.rank1(2));
    assert(rs.rank1(3) == 2);
    assert(rs.rank1(3) == simple.rank1(3));
    assert(rs.rank1(4) == 3);
    assert(rs.rank1(4) == simple.rank1(4));
    assert(rs.rank1(5) == 3);
    assert(rs.rank1(5) == simple.rank1(5));
  }
  {
    assert(rs.select0(0) == 1);
    assert(rs.select0(0) == simple.select0(0));
    assert(rs.select0(1) == 4);
    assert(rs.select0(1) == simple.select0(1));
    assert(rs.select0(2) == 5);
    assert(rs.select0(2) == simple.select0(2));
  }
  {
    assert(rs.select1(0) == 0);
    assert(rs.select1(0) == simple.select1(0));
    assert(rs.select1(1) == 2);
    assert(rs.select1(1) == simple.select1(1));
    assert(rs.select1(2) == 3);
    assert(rs.select1(2) == simple.select1(2));
  }
  {
    assert(rs.max_rank0() == 3);
    assert(rs.max_rank0() == simple.max_rank0());
    assert(rs.max_rank1() == 3);
    assert(rs.max_rank1() == simple.max_rank1());
  }

  printf("\tdone\n");
}

/*
 * pos: 0 1 2 3 4 5
 *      1 0 1 1 0 0
 */
void test_fewone_risk() {
  printf("==== Test fewone @ last is '0' && risk mmap data\n");
  const size_t kCnt = 6;
  //terark::rank_select_simple simple(kCnt);
  terark::rank_select_il_256_32 simple(kCnt);
  size_t pos = 0;
  simple.set1(pos++); simple.set0(pos++);
  simple.set1(pos++); simple.set1(pos++);
  simple.set0(pos++); simple.set0(pos++);
  simple.build_cache(true, true);

  rank_select_fewone<uint32_t> rs_a(kCnt);
  rs_a.build_from(simple);

  size_t memsz = rs_a.mem_size();
  assert(memsz % 8 == 0);
  rank_select_fewone<uint32_t> rs(kCnt);
  rs.risk_mmap_from((unsigned char*)rs_a.data(), memsz);
  rs_a.risk_release_ownership();
  
  {
    size_t memsz = rs.mem_size();
    assert(memsz % 8 == 0);
  }
  {
    size_t hint;
    assert(rs.zero_seq_len(0, hint) == 0);
    assert(rs.zero_seq_len(0, hint) == simple.zero_seq_len(0));
    assert(rs.zero_seq_len(1, hint) == 1);
    assert(rs.zero_seq_len(1, hint) == simple.zero_seq_len(1));
    assert(rs.zero_seq_len(2, hint) == 0);
    assert(rs.zero_seq_len(2, hint) == simple.zero_seq_len(2));
    assert(rs.zero_seq_len(3, hint) == 0);
    assert(rs.zero_seq_len(3, hint) == simple.zero_seq_len(3));
    assert(rs.zero_seq_len(4, hint) == 2);
    //assert(rs.zero_seq_len(4, hint) == simple.zero_seq_len(4));
    assert(rs.zero_seq_len(5, hint) == 1);
    //assert(rs.zero_seq_len(5, hint) == simple.zero_seq_len(5));
  }
  {
    size_t hint;
    assert(rs.zero_seq_revlen(0, hint) == 0);
    assert(rs.zero_seq_revlen(0, hint) == simple.zero_seq_revlen(0));
    assert(rs.zero_seq_revlen(1, hint) == 0);
    assert(rs.zero_seq_revlen(1, hint) == simple.zero_seq_revlen(1));
    assert(rs.zero_seq_revlen(2, hint) == 1);
    assert(rs.zero_seq_revlen(2, hint) == simple.zero_seq_revlen(2));
    assert(rs.zero_seq_revlen(3, hint) == 0);
    assert(rs.zero_seq_revlen(3, hint) == simple.zero_seq_revlen(3));
    assert(rs.zero_seq_revlen(4, hint) == 0);
    assert(rs.zero_seq_revlen(4, hint) == simple.zero_seq_revlen(4));
    assert(rs.zero_seq_revlen(5, hint) == 1);
    assert(rs.zero_seq_revlen(5, hint) == simple.zero_seq_revlen(5));
  }

  {
    assert(rs.zero_seq_revlen(0) == 0);
    assert(rs.zero_seq_revlen(0) == simple.zero_seq_revlen(0));
    assert(rs.zero_seq_revlen(1) == 0);
    assert(rs.zero_seq_revlen(1) == simple.zero_seq_revlen(1));
    assert(rs.zero_seq_revlen(2) == 1);
    assert(rs.zero_seq_revlen(2) == simple.zero_seq_revlen(2));
    assert(rs.zero_seq_revlen(3) == 0);
    assert(rs.zero_seq_revlen(3) == simple.zero_seq_revlen(3));
    assert(rs.zero_seq_revlen(4) == 0);
    assert(rs.zero_seq_revlen(4) == simple.zero_seq_revlen(4));
    assert(rs.zero_seq_revlen(5) == 1);
    assert(rs.zero_seq_revlen(5) == simple.zero_seq_revlen(5));
  }
  {
    assert(rs.one_seq_revlen(0) == 0);
    assert(rs.one_seq_revlen(0) == simple.one_seq_revlen(0));
    assert(rs.one_seq_revlen(1) == 1);
    assert(rs.one_seq_revlen(1) == simple.one_seq_revlen(1));
    assert(rs.one_seq_revlen(2) == 0);
    assert(rs.one_seq_revlen(2) == simple.one_seq_revlen(2));
    assert(rs.one_seq_revlen(3) == 1);
    assert(rs.one_seq_revlen(3) == simple.one_seq_revlen(3));
    assert(rs.one_seq_revlen(4) == 2);
    assert(rs.one_seq_revlen(4) == simple.one_seq_revlen(4));
    assert(rs.one_seq_revlen(5) == 0);
    assert(rs.one_seq_revlen(5) == simple.one_seq_revlen(5));
  }
  {
    assert(rs.zero_seq_len(0) == 0);
    assert(rs.zero_seq_len(0) == simple.zero_seq_len(0));
    assert(rs.zero_seq_len(1) == 1);
    assert(rs.zero_seq_len(1) == simple.zero_seq_len(1));
    assert(rs.zero_seq_len(2) == 0);
    assert(rs.zero_seq_len(2) == simple.zero_seq_len(2));
    assert(rs.zero_seq_len(3) == 0);
    assert(rs.zero_seq_len(3) == simple.zero_seq_len(3));
    assert(rs.zero_seq_len(4) == 2);
    //assert(rs.zero_seq_len(4) == simple.zero_seq_len(4));
    assert(rs.zero_seq_len(5) == 1);
    //assert(rs.zero_seq_len(5) == simple.zero_seq_len(5));
  }
  {
    assert(rs.one_seq_len(0) == 1);
    assert(rs.one_seq_len(0) == simple.one_seq_len(0));
    assert(rs.one_seq_len(1) == 0);
    assert(rs.one_seq_len(1) == simple.one_seq_len(1));
    assert(rs.one_seq_len(2) == 2);
    assert(rs.one_seq_len(2) == simple.one_seq_len(2));
    assert(rs.one_seq_len(3) == 1);
    assert(rs.one_seq_len(3) == simple.one_seq_len(3));
    assert(rs.one_seq_len(4) == 0);
    assert(rs.one_seq_len(4) == simple.one_seq_len(4));
    assert(rs.one_seq_len(5) == 0);
    assert(rs.one_seq_len(5) == simple.one_seq_len(5));
  }
  {
    assert(rs.is0(0) == false);
    assert(rs.is0(0) == simple.is0(0));
    assert(rs.is0(1) == true);
    assert(rs.is0(1) == simple.is0(1));
    assert(rs.is0(2) == false);
    assert(rs.is0(2) == simple.is0(2));
    assert(rs.is0(3) == false);
    assert(rs.is0(3) == simple.is0(3));
    assert(rs.is0(4) == true);
    assert(rs.is0(4) == simple.is0(4));
    assert(rs.is0(5) == true);
    assert(rs.is0(5) == simple.is0(5));
  }
  {
    assert(rs.is1(0) == true);
    assert(rs.is1(0) == simple.is1(0));
    assert(rs.is1(1) == false);
    assert(rs.is1(1) == simple.is1(1));
    assert(rs.is1(2) == true);
    assert(rs.is1(2) == simple.is1(2));
    assert(rs.is1(3) == true);
    assert(rs.is1(3) == simple.is1(3));
    assert(rs.is1(4) == false);
    assert(rs.is1(4) == simple.is1(4));
    assert(rs.is1(5) == false);
    assert(rs.is1(5) == simple.is1(5));
  }
  {
    assert(rs.rank0(0) == 0);
    assert(rs.rank0(0) == simple.rank0(0));
    assert(rs.rank0(1) == 0);
    assert(rs.rank0(1) == simple.rank0(1));
    assert(rs.rank0(2) == 1);
    assert(rs.rank0(2) == simple.rank0(2));
    assert(rs.rank0(3) == 1);
    assert(rs.rank0(3) == simple.rank0(3));
    assert(rs.rank0(4) == 1);
    assert(rs.rank0(4) == simple.rank0(4));
    assert(rs.rank0(5) == 2);
    assert(rs.rank0(5) == simple.rank0(5));
  }
  {
    assert(rs.rank1(0) == 0);
    assert(rs.rank1(0) == simple.rank1(0));
    assert(rs.rank1(1) == 1);
    assert(rs.rank1(1) == simple.rank1(1));
    assert(rs.rank1(2) == 1);
    assert(rs.rank1(2) == simple.rank1(2));
    assert(rs.rank1(3) == 2);
    assert(rs.rank1(3) == simple.rank1(3));
    assert(rs.rank1(4) == 3);
    assert(rs.rank1(4) == simple.rank1(4));
    assert(rs.rank1(5) == 3);
    assert(rs.rank1(5) == simple.rank1(5));
  }
  {
    assert(rs.select0(0) == 1);
    assert(rs.select0(0) == simple.select0(0));
    assert(rs.select0(1) == 4);
    assert(rs.select0(1) == simple.select0(1));
    assert(rs.select0(2) == 5);
    assert(rs.select0(2) == simple.select0(2));
  }
  {
    assert(rs.select1(0) == 0);
    assert(rs.select1(0) == simple.select1(0));
    assert(rs.select1(1) == 2);
    assert(rs.select1(1) == simple.select1(1));
    assert(rs.select1(2) == 3);
    assert(rs.select1(2) == simple.select1(2));
  }
  {
    assert(rs.max_rank0() == 3);
    assert(rs.max_rank0() == simple.max_rank0());
    assert(rs.max_rank1() == 3);
    assert(rs.max_rank1() == simple.max_rank1());
  }

  printf("\tdone\n");
}


int main() {
  printf("EXAGGERATE\n");
  test_fewzero();
  test_fewone();

  test_fewzero_risk();
  test_fewone_risk();
  return 0;
}
