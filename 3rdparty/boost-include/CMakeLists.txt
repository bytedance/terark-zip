CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

INCLUDE(ExternalProject)
if(TARGET boost-project)
    MESSAGE(STATUS "boost-project target existed, re-use it!")
else()
    ExternalProject_Add(boost-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ""
        INSTALL_COMMAND bash bootstrap.sh --with-libraries=none
    )
endif()

if(TARGET boost-system)
    MESSAGE(STATUS "boost-system target existed, re-use it!")
else()
    ExternalProject_Add(boost-system-build
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ./b2 --build-dir=${CMAKE_CURRENT_BINARY_DIR}/boost-system --stagedir=${CMAKE_CURRENT_BINARY_DIR} cxxflags=-fPIC cxxflags=-std=gnu++14 cflags=-fPIC threading=multi link=static variant=release visibility=global --with-system
        INSTALL_COMMAND ""
    )
    ADD_DEPENDENCIES(boost-system-build boost-project)

    ADD_LIBRARY(boost-system STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-system boost-system-build)
    SET_TARGET_PROPERTIES(boost-system PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_system.a
    )
endif()

if(TARGET boost-fiber)
    MESSAGE(STATUS "boost-fiber target existed, re-use it!")
else()
    ExternalProject_Add(boost-fiber-build
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ./b2 --build-dir=${CMAKE_CURRENT_BINARY_DIR}/boost-fiber --stagedir=${CMAKE_CURRENT_BINARY_DIR} cxxflags=-fPIC cxxflags=-std=gnu++14 cflags=-fPIC threading=multi link=static variant=release visibility=global --with-fiber
        INSTALL_COMMAND ""
    )
    ADD_DEPENDENCIES(boost-fiber-build boost-project)

    ADD_LIBRARY(boost-fiber-fiber STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-fiber-fiber boost-fiber-build)
    SET_TARGET_PROPERTIES(boost-fiber-fiber PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_fiber.a
    )
    ADD_LIBRARY(boost-fiber-context STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-fiber-context boost-fiber-build)
    SET_TARGET_PROPERTIES(boost-fiber-context PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_context.a
    )
    ADD_LIBRARY(boost-fiber-filesystem STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-fiber-filesystem boost-fiber-build)
    SET_TARGET_PROPERTIES(boost-fiber-filesystem PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_filesystem.a
    )
    add_library(boost-fiber INTERFACE IMPORTED GLOBAL)
    set_property(TARGET boost-fiber PROPERTY
        INTERFACE_LINK_LIBRARIES boost-fiber-fiber boost-fiber-context boost-fiber-filesystem
    )
endif()

if(TARGET boost-thread)
    MESSAGE(STATUS "boost-thread target existed, re-use it!")
else()
    ExternalProject_Add(boost-thread-build
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ./b2 --build-dir=${CMAKE_CURRENT_BINARY_DIR}/boost-thread --stagedir=${CMAKE_CURRENT_BINARY_DIR} cxxflags=-fPIC cxxflags=-std=gnu++14 cflags=-fPIC threading=multi link=static variant=release visibility=global --with-thread
        INSTALL_COMMAND ""
    )
    ADD_DEPENDENCIES(boost-thread-build boost-project)

    ADD_LIBRARY(boost-thread STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-thread boost-thread-build)
    SET_TARGET_PROPERTIES(boost-thread PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_thread.a
    )
endif()

if(TARGET boost-test)
    MESSAGE(STATUS "boost-test target existed, re-use it!")
else()
    ExternalProject_Add(boost-test-build
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ./b2 --build-dir=${CMAKE_CURRENT_BINARY_DIR}/boost-test --stagedir=${CMAKE_CURRENT_BINARY_DIR} cxxflags=-fPIC cxxflags=-std=gnu++14 cflags=-fPIC threading=multi link=static variant=release visibility=global --with-test
        INSTALL_COMMAND ""
    )
    ADD_DEPENDENCIES(boost-test-build boost-project)

    ADD_LIBRARY(boost-test-vprg_exec_monitor STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-test-vprg_exec_monitor boost-test-build)
    SET_TARGET_PROPERTIES(boost-test-vprg_exec_monitor PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_vprg_exec_monitor.a
    )
    ADD_LIBRARY(boost-test-test_exec_monitor STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-test-test_exec_monitor boost-test-build)
    SET_TARGET_PROPERTIES(boost-test-test_exec_monitor PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost-test_exec_monitor.a
    )
    ADD_LIBRARY(boost-test-unit_test_framework STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-test-unit_test_framework boost-test-build)
    SET_TARGET_PROPERTIES(boost-test-unit_test_framework PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_unit_test_framework.a
    )
    add_library(boost-test INTERFACE IMPORTED GLOBAL)
    set_property(TARGET boost-test PROPERTY
        INTERFACE_LINK_LIBRARIES boost-test-vprg_exec_monitor boost-test-test_exec_monitor boost-test-unit_test_framework
    )
endif()


