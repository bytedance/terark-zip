#include <gtest/gtest.h>
#include <stdio.h>
#include <terark/bitmap.hpp>
#include <terark/rank_select.hpp>

#define RS_REG_TEST(__P, __W) \
    TEST(RANK_SELECT_FEW_REG_TEST, PIVOT_##__P##_WIDTH_##__W){ \
        rank_select_few<__P, __W> rs; \
        rank_select_few_builder<__P, __W> rsbuild(50, 3276849, false); \
        for(int i = 0; i < 3276899; i++) \
            if(i < 588939 || i > 588988) rsbuild.insert(i); \
        rsbuild.finish(&rs); \
        for(int i = 0; i < 50; i++){ \
            ASSERT_TRUE(rs.select0(i) == 588939+i); \
            ASSERT_TRUE(rs.rank0(588939+i) == i); \
            ASSERT_TRUE(rs.zero_seq_revlen(588939+i) == i); \
        } \
        ASSERT_TRUE(rs.zero_seq_revlen(588989) == 50); \
        ASSERT_TRUE(rs.zero_seq_revlen(588990) == 0); \
        ASSERT_TRUE(rs.zero_seq_len(3276799) == 0); \
        ASSERT_TRUE(rs.zero_seq_len(3276800) == 0); \
        ASSERT_TRUE(rs.zero_seq_len(3276801) == 0); \
        ASSERT_TRUE(rs.is1(3276799)); \
        ASSERT_TRUE(rs.is1(3276800)); \
        ASSERT_TRUE(rs.is1(3276801)); \
    }

namespace terark {
    RS_REG_TEST(0, 3)
    RS_REG_TEST(0, 4)
    RS_REG_TEST(0, 5)
    RS_REG_TEST(0, 6)
    RS_REG_TEST(0, 7)
    RS_REG_TEST(0, 8)
}