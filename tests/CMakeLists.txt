CMAKE_MINIMUM_REQUIRED(VERSION 3.6)
PROJECT(terark-core-test)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++1y")
SET(CMAKE_CXX_STANDARD 14)

MESSAGE("googletest include dir: ${GTEST_INC}")
MESSAGE("googletest library dir: ${GTEST_LIB}")

INCLUDE_DIRECTORIES(${GTEST_INC})
INCLUDE_DIRECTORIES(${BOOST_INC})
INCLUDE_DIRECTORIES(../src)

LINK_DIRECTORIES(${GTEST_LIB})
LINK_DIRECTORIES(${BOOST_LIB})

FILE(GLOB_RECURSE TEST_SRC "../src/*_test.cpp" "*_test.cpp")
FILE(GLOB_RECURSE MAIN_SRC "../src/*.*")

MESSAGE("${BOOST_INC}")

FOREACH (test_file ${TEST_SRC})
    LIST(REMOVE_ITEM MAIN_SRC ${test_file})
ENDFOREACH ()

ENABLE_TESTING()
FUNCTION(test_func test_file)
    SET(extra_args ${ARGN})
    GET_FILENAME_COMPONENT(test_target_name "${test_file}" NAME_WE)
    ADD_EXECUTABLE("${test_target_name}" "${test_file}" "${MAIN_SRC}")
    TARGET_LINK_LIBRARIES("${test_target_name}" boost_filesystem boost_fiber gomp aio rt gtest_main gmock "${extra_args}")
    ADD_TEST(NAME "${test_target_name}" COMMAND "${test_target_name}")
ENDFUNCTION(test_func)


FOREACH (test_item ${TEST_SRC})
  MESSAGE("${test_item}")
  test_func(${test_item})
ENDFOREACH ()
