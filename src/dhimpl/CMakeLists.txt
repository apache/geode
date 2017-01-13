cmake_minimum_required(VERSION 3.4)
project(dhimpl)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(DHImpl SHARED ${SOURCES})
target_link_libraries(DHImpl
  PUBLIC 
    gfcppcache
    crypto
    c++11
)
