cmake_minimum_required(VERSION 3.4)
project(gfcpp)

file(GLOB_RECURSE SOURCES "*.cpp")

add_executable(gfcpp ${SOURCES})

target_link_libraries(gfcpp gfcppcache)

#add_dependencies(gfcpp gfcppcache )

#TODO move cppcahce headers?
include_directories(${CMAKE_BINARY_DIR}/cppcache)
include_directories(${CMAKE_SOURCE_DIR}/cppcache)
