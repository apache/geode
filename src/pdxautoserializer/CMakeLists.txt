cmake_minimum_required(VERSION 3.4)
project(pdxautoserializer)

file(GLOB_RECURSE SOURCES "*.cpp")

add_executable(pdxautoserializer ${SOURCES})
target_link_libraries(pdxautoserializer
  PRIVATE
    antlr
  PUBLIC
    gfcppcache
    c++11
)

install(TARGETS pdxautoserializer DESTINATION bin)
