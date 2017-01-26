cmake_minimum_required(VERSION 3.4)
project(gfcpp)

file(GLOB_RECURSE SOURCES "*.cpp")

add_executable(gfcpp ${SOURCES})

target_link_libraries(gfcpp apache-geode c++11)

install(TARGETS gfcpp DESTINATION bin)
