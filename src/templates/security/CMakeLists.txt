cmake_minimum_required(VERSION 3.4)
project(templates.security)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(securityImpl SHARED ${SOURCES} CMakeLists.txt.forInstall)

target_link_libraries(securityImpl
  PUBLIC
    gfcppcache
    crypto
)
