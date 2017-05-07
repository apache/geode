cmake_minimum_required(VERSION 3.4)
project(pdxautoserializer)

file(GLOB_RECURSE SOURCES "*.cpp")

add_executable(pdxautoserializer ${SOURCES})

target_link_libraries(pdxautoserializer
  PUBLIC
    gfcppcache
  PRIVATE
    ${antlr_STATIC_LIB}
)

add_dependencies(pdxautoserializer antlr)

#TODO break out headers?
include_directories(${CMAKE_BINARY_DIR}/cppcache)
#TODO fix dependencies include paths
include_directories(${CMAKE_SOURCE_DIR}/cppcache)
include_directories(${DEPENDENCIES_antlr_DIR}/include)

