cmake_minimum_required(VERSION 3.4)
project(sqliteimpl)

file(GLOB_RECURSE SOURCES "*.cpp")

link_directories(${DEPENDENCIES_sqlite_DIR}/lib)

add_library(SqLiteImpl SHARED ${SOURCES})

#TODO get external project library names
target_link_libraries(SqLiteImpl
        PUBLIC
            gfcppcache
            ${sqlite_SHARED_LIB}
# TODO windows SHARED_LIB resolves to DLL and not import LIB
# TODO consider static sqlite3   ${sqlite_SHARED_LIB}
# transitive           ${RUNTIME_LIBRARIES}
)

add_dependencies(SqLiteImpl sqlite)

#TODO is this correct for config.h
include_directories(${CMAKE_BINARY_DIR}/cppcache)
#TODO fix dependencies include paths
include_directories(${CMAKE_SOURCE_DIR}/cppcache)
include_directories(${DEPENDENCIES_sqlite_DIR}/include)
