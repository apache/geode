cmake_minimum_required(VERSION 3.4)
project(cryptoimpl)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(cryptoImpl SHARED ${SOURCES})

target_link_libraries(cryptoImpl
  PRIVATE
    ACE_SSL
  PUBLIC
   gfcppcache
   crypto
   ssl
   c++11
)

if ("Windows" STREQUAL ${CMAKE_SYSTEM_NAME} )
  install(TARGETS cryptoImpl
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib)
else()
  install (TARGETS cryptoImpl DESTINATION lib)
endif()

