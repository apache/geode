cmake_minimum_required(VERSION 3.4)
project(dhimpl)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(DHImpl SHARED ${SOURCES})

#TODO get external project library names
target_link_libraries(DHImpl
        PUBLIC 
            gfcppcache
            ${openssl_CRYPTO_SHARED_LIB}
# transitive            ${RUNTIME_LIBRARIES}
)

add_dependencies(DHImpl openssl)

#TODO is this correct for config.h
include_directories(${CMAKE_BINARY_DIR}/cppcache)
#TODO fix dependencies include paths
include_directories(${CMAKE_SOURCE_DIR}/cppcache)
include_directories(${DEPENDENCIES_openssl_DIR}/include)
