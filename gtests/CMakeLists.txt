CMAKE_MINIMUM_REQUIRED(VERSION 3.6)
PROJECT(terark-core-gtest)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++1y -fsanitize=address")
SET(CMAKE_CXX_STANDARD 14)

#MESSAGE("googletest include dir: ${GTEST_INC}")
#MESSAGE("googletest library dir: ${GTEST_LIB_DIR}")

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../3rdparty/googletest/googletest/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../output/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../src)


ADD_SUBDIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/googletest)

LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../output/lib_static)

SET(TEST_SRC "simple_test.cpp"
             "utils_test.cpp"
             "zbs/zbs_test.cpp")

SET(TERARK_LIBS "-lterark-idx-d -lterark-zbs-d -lterark-fsa-d -lterark-core-d")

ENABLE_TESTING()
FUNCTION(test_func test_file)
    SET(extra_args ${ARGN})
    GET_FILENAME_COMPONENT(test_target_name "${test_file}" NAME_WE)
    ADD_EXECUTABLE("${test_target_name}" "${test_file}" "${MAIN_SRC}")
    TARGET_LINK_LIBRARIES("${test_target_name}" ${TERARK_LIBS} gomp aio rt gtest_main gmock_main gtest "${extra_args}")
    ADD_TEST(NAME "${test_target_name}" COMMAND "${test_target_name}")
ENDFUNCTION(test_func)


FOREACH (test_item ${TEST_SRC})
  MESSAGE("${test_item}")
  test_func(${test_item})
ENDFOREACH ()
