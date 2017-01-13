cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests.fwk)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(fwk SHARED ${SOURCES})
set_target_properties(fwk PROPERTIES FOLDER test)

target_link_libraries(fwk
  PRIVATE
    ACE
  PUBLIC 
    framework
    c++11
)

#TODO very bad we include root of tests
include_directories(${CMAKE_SOURCE_DIR}/tests/cpp)
