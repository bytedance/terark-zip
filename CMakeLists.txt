CMAKE_MINIMUM_REQUIRED(VERSION 3.6)
PROJECT(terark-core)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
SET(CMAKE_CXX_STANDARD 14)

# Headers
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/src)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/zstd)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/boost-include)

SET(BUILD_SUFFIX "d")
IF(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
  SET(BUILD_SUFFIX "r")
ENDIF()

INCLUDE(ExternalProject)
ExternalProject_Add(boost-project
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/boost-include
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE 1
    BUILD_COMMAND bash bootstrap.sh --prefix=${CMAKE_CURRENT_BINARY_DIR}/boost --with-libraries=fiber,context,system,filesystem
    INSTALL_COMMAND ./b2 "cxxflags=-fPIC -std=gnu++14" -j20 "cflags=-fPIC" threading=multi link=static variant=release)

ADD_LIBRARY(boost-fiber STATIC IMPORTED GLOBAL)
ADD_DEPENDENCIES(boost-fiber boost-project)
SET_TARGET_PROPERTIES(boost-fiber PROPERTIES
    IMPORTED_LOCATION ${CMAKE_CURRENT_SOURCE_DIR}/boost-include/stage/lib/libboost_fiber.a
    INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/boost-include)

ADD_LIBRARY(boost-context STATIC IMPORTED GLOBAL)
ADD_DEPENDENCIES(boost-context boost-project)
SET_TARGET_PROPERTIES(boost-context PROPERTIES
    IMPORTED_LOCATION ${CMAKE_CURRENT_SOURCE_DIR}/boost-include/stage/lib/libboost_context.a
    INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/boost-include)

ADD_LIBRARY(boost-system STATIC IMPORTED GLOBAL)
ADD_DEPENDENCIES(boost-system boost-project)
SET_TARGET_PROPERTIES(boost-system PROPERTIES
    IMPORTED_LOCATION ${CMAKE_CURRENT_SOURCE_DIR}/boost-include/stage/lib/libboost_system.a
    INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/boost-include)

ADD_LIBRARY(boost-filesystem STATIC IMPORTED GLOBAL)
ADD_DEPENDENCIES(boost-filesystem boost-project)
SET_TARGET_PROPERTIES(boost-filesystem PROPERTIES
    IMPORTED_LOCATION ${CMAKE_CURRENT_SOURCE_DIR}/boost-include/stage/lib/libboost_filesystem.a
    INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/boost-include)

# CORE LIB
FILE(GLOB CORE_SRC src/terark/*.cpp
        src/terark/io/*.cpp
        src/terark/util/*.cpp
        src/terark/thread/*.cpp
        src/terark/succinct/*.cpp
        src/terark/c/*.c
        boost-include/libs/coroutine/src/*.cpp
        boost-include/libs/coroutine/src/detail/*.cpp
        boost-include/libs/coroutine/src/posix/*.cpp
        boost-include/libs/fiber/src/*.cpp
        boost-include/libs/fiber/src/algo/*.cpp
        boost-include/libs/fiber/src/numa/*.cpp
        # boost-include/libs/fiber/examples/*.cpp
        # boost-include/libs/fiber/examples/asio/*.cpp
        # boost-include/libs/fiber/examples/cuda/*.cpp
        # boost-include/libs/fiber/examples/hip/*.cpp
        # boost-include/libs/fiber/examples/numa/*.cpp
        )

# FSA LIB
FILE(GLOB FSA_SRC src/terark/fsa/*.cpp)

# Embbed ZSTD
FILE(GLOB ZSTD_SRC 3rdparty/zstd/zstd/common/*.c
        3rdparty/zstd/zstd/compress/*.c
        3rdparty/zstd/zstd/decompress/*.c
        3rdparty/zstd/zstd/deprecated/*.c
        3rdparty/zstd/zstd/dictBuilder/*.c
        3rdparty/zstd/zstd/legacy/*.c)

# ZBS LIB
FILE(GLOB ZBS_SRC src/terark/entropy/*.cpp
        src/terark/zbs/*.cpp
        ${ZSTD_SRC})

# IDX LIB
FILE(GLOB IDX_SRC src/terark/idx/*.cpp)

# Combined all into libterark-core.a
ADD_LIBRARY(terark-core-${BUILD_SUFFIX} STATIC ${CORE_SRC} ${IDX_SRC} ${ZBS_SRC} ${FSA_SRC})
IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
        # MacOS doesn't need libaio and rt
        TARGET_LINK_LIBRARIES(terark-core-${BUILD_SUFFIX} boost-fiber boost-filesystem boost-context boost-system pthread)
ELSE()
        TARGET_LINK_LIBRARIES(terark-core-${BUILD_SUFFIX} boost-fiber boost-filesystem boost-context boost-system aio pthread rt)
ENDIF()
TARGET_INCLUDE_DIRECTORIES(terark-core-${BUILD_SUFFIX} PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>"
                                                              "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/zstd>"
                                                              # expose boost headers in case someone need it
                                                              "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/boost-include>")


INSTALL(TARGETS terark-core-${BUILD_SUFFIX}
        EXPORT terark-core-${BUILD_SUFFIX}-targets
        ARCHIVE DESTINATION ${CMAKE_BINARY_DIR}/terark-core/lib
        )

INSTALL( EXPORT terark-core-${BUILD_SUFFIX}-targets DESTINATION ${CMAKE_BINARY_DIR}/terark-core/cmake)

INSTALL(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/terark
        DESTINATION ${CMAKE_BINARY_DIR}/terark-core/include
        FILES_MATCHING 
        PATTERN "*.h"
        PATTERN "*.hpp"
)

INSTALL(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/zstd/zstd
        DESTINATION ${CMAKE_BINARY_DIR}/terark-core/include
        FILES_MATCHING 
        PATTERN "*.h"
        PATTERN "*.hpp"
)


############## Tests ##############
IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    # tests
    FILE(GLOB TESTS_SRC
            tests/core/*.cpp
            tests/tries/*.cpp
            tests/succinct/*.cpp)
    ADD_LIBRARY(terark-tests SHARED ${TESTS_SRC})
    TARGET_LINK_LIBRARIES(terark-tests terark-core-${BUILD_SUFFIX})

    # tools
    FILE(GLOB TOOLS_SRC
            tools/general/*.cpp
            tools/fsa/*.cpp
            tools/zbs/*.cpp)
    ADD_LIBRARY(terark-tools SHARED ${TOOLS_SRC})
    TARGET_LINK_LIBRARIES(terark-tools terark-core-${BUILD_SUFFIX})
ENDIF ()

############## Examples & Tools  ##############
# TODO
