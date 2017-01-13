cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests.fwklib)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(framework STATIC ${SOURCES})
set_target_properties(framework PROPERTIES FOLDER test)

target_link_libraries(framework
  PRIVATE
    ACE
  PUBLIC 
    gfcppcache
    xerces-c
)

# Add PIC flags to link later with shared library
add_definitions(${CMAKE_CXX_COMPILE_OPTIONS_PIC})

#TODO very bad we include root of tests
include_directories(${CMAKE_SOURCE_DIR}/tests/cpp)
