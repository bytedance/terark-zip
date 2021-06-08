CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

INCLUDE(ExternalProject)
if(TARGET boost-project)
    MESSAGE(STATUS "boost-project target existed, re-use it!")
else()
    ExternalProject_Add(boost-project
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
        CONFIGURE_COMMAND ./bootstrap.sh --with-libraries=system,fiber,thread
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ./b2 --build-dir=${CMAKE_CURRENT_BINARY_DIR} --stagedir=${CMAKE_CURRENT_BINARY_DIR} cxxflags=-fPIC -std=gnu++14 cflags=-fPIC threading=multi link=static variant=release visibility=global
        INSTALL_COMMAND ""
    )
    ADD_LIBRARY(boost-system STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-system boost-project)
    SET_TARGET_PROPERTIES(boost-system PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_system.a
    )
    ADD_LIBRARY(boost-fiber-fiber STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-fiber-fiber boost-project)
    SET_TARGET_PROPERTIES(boost-fiber-fiber PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_fiber.a
    )
    ADD_LIBRARY(boost-fiber-context STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-fiber-context boost-project)
    SET_TARGET_PROPERTIES(boost-fiber-context PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_context.a
    )
    ADD_LIBRARY(boost-fiber-filesystem STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-fiber-filesystem boost-project)
    SET_TARGET_PROPERTIES(boost-fiber-filesystem PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_filesystem.a
    )
    add_library(boost-fiber INTERFACE IMPORTED GLOBAL)
    set_property(TARGET boost-fiber PROPERTY
        INTERFACE_LINK_LIBRARIES boost-fiber-fiber boost-fiber-context boost-fiber-filesystem
    )
    ADD_LIBRARY(boost-thread STATIC IMPORTED GLOBAL)
    ADD_DEPENDENCIES(boost-thread boost-project)
    SET_TARGET_PROPERTIES(boost-thread PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/lib/libboost_thread.a
    )
endif()
