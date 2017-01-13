cmake_minimum_required(VERSION 3.4)
project(sqliteimpl)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(SqLiteImpl SHARED ${SOURCES})
target_link_libraries(SqLiteImpl
  PUBLIC
    gfcppcache
    sqlite
    c++11
)
