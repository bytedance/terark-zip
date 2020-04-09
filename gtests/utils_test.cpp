#include "gtest/gtest.h"
#include "utils.hpp"

TEST(UTILS_TEST, FILE_EXISTS) {
    std::cout << 0 << " " << terark::file_exist("/Users/guokuankuan/Programs/terark-tools/123") << std::endl;
    std::cout << 1 << " " << terark::file_exist("/Users/guokuankuan/Programs/terark-tools/README.md") << std::endl;
    std::cout << 1 << " " << terark::file_exist("/Users/guokuankuan/Programs/terark-tools/CmakeLists.txt") << std::endl;
    std::cout << 0 << " " << terark::file_exist("/Users/guokuankuan/Programs/terark-tools/not_exist") << std::endl;
}
