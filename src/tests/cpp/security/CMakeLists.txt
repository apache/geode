cmake_minimum_required(VERSION 3.4)
project(tests.security)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(security STATIC ${SOURCES})

target_link_libraries(security
  PRIVATE
    ACE
  PUBLIC
    fwk
    pdxobject
    testobject
    crypto
    ssl
)

set_target_properties(security PROPERTIES FOLDER test)

# TODO fix includes
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
