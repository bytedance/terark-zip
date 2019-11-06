#include <gtest/gtest.h>
#include <getopt.h>
#include <random>

#include "terark/util/sortable_strvec.hpp"
#include "utils.h"

namespace terark {

  TEST(SORTABLE_STRVEC_TEST, SIMPLE_TEST) {
    SortableStrVec strVec;
    strVec.push_back("1");
    strVec.push_back("2");
    strVec.push_back("2");
    strVec.push_back("3");
    strVec.push_back("4");
    strVec.push_back("5");
    strVec.push_back("5");
    strVec.push_back("6");
    strVec.push_back("7");

    ASSERT_TRUE(strVec.lower_bound("2") == 1);
    ASSERT_TRUE(strVec.lower_bound("4") == 4);
    ASSERT_TRUE(strVec.lower_bound("0") == 0);
  }
  /**
   * Helper function equivelent to std::lower_bound
   */
  int lower_bound(int arr[], int target, int arr_len ) {
    if(arr_len == 0) return 0;
    int l = 0;
    int h = arr_len;
    int mid = 0;
    while(l < h) {
      mid = l + (h - l) / 2; 
      if(arr[mid] >= target) {
        h = mid;
      } else {
        l = mid + 1;
      }
    }
    return l;
  }

  // helper function, not useful
  int upper_bound(int arr[], int target, int arr_len) {
    if(arr_len == 0) return 0;
    int l = 0;
    int h = arr_len;
    int mid = 0;
    while(l < h) {
      mid = (h + l) / 2;
    }
    return l;
  }

  TEST(LOWER_BOUND_TEST, SIMPLE_TEST) {
    int arr[9] = {1,2,2,3,4,5,5,6,7};
    ASSERT_TRUE(lower_bound(arr, 2, 9) == 1);
    ASSERT_TRUE(lower_bound(arr, 3, 9) == 3);
    ASSERT_TRUE(lower_bound(arr, 4, 9) == 4);
    ASSERT_TRUE(lower_bound(arr, 10, 9) == 9);
    ASSERT_TRUE(lower_bound(arr, 0, 9) == 0);
    ASSERT_TRUE(lower_bound(arr, -10, 9) == 0);
  }

  TEST(LOWER_BOUND_TEST, MORE_TEST) {
    int arr[13] = {-10,-2,2,3,4,5,5,6,7,7,7,7,7};
    ASSERT_TRUE(lower_bound(arr, 2, 13) == 2);
    ASSERT_TRUE(lower_bound(arr, 7, 13) == 8);
  }

}

