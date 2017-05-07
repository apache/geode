cmake_minimum_required(VERSION 3.4)
project(templates.security)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(securityImpl SHARED ${SOURCES})

target_link_libraries(securityImpl
        PUBLIC
            gfcppcache
            ${openssl_CRYPTO_SHARED_LIB}
)

add_dependencies(securityImpl openssl)

#TODO is this correct for config.h
include_directories(${CMAKE_BINARY_DIR}/cppcache)
#TODO fix dependencies include paths
include_directories(${CMAKE_SOURCE_DIR}/cppcache)
include_directories(${CMAKE_BINARY_DIR}/cppcache/include)
include_directories(${DEPENDENCIES_openssl_DIR}/include)
