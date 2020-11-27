#include <iostream>
#include <string>
#include "gtest/gtest.h"

namespace terark {
  class ValvecTest: public testing::Test {
  };

  TEST_F(ValvecTest, BasicTest) {
    std::cout << "valvectest::basictest" << std::endl;
  }
} // namespace terark

int main(int argc, char** argv) {
  std::cout << "run tests" << std::endl;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
