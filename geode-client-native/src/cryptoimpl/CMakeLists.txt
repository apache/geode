cmake_minimum_required(VERSION 3.4)
project(cryptoimpl)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(cryptoImpl SHARED ${SOURCES})

target_link_libraries(cryptoImpl
        PRIVATE
            ${ACE_STATIC_LIB}
            ${ACE_SSL_STATIC_LIB}
        PUBLIC
            gfcppcache
            ${openssl_CRYPTO_SHARED_LIB}
            ${openssl_SSL_SHARED_LIB}
# transitive           ${RUNTIME_LIBRARIES}
)

#TODO move to config.h when not building shared ace
add_definitions(-D__ACE_INLINE__)
add_definitions(-DACE_AS_STATIC_LIBS)

add_dependencies(cryptoImpl ACE openssl)

#TODO is this correct for config.h
include_directories(${CMAKE_BINARY_DIR}/cppcache)
#TODO fix dependencies include paths
include_directories(${CMAKE_SOURCE_DIR}/cppcache)
include_directories(${DEPENDENCIES_ACE_DIR}/include)
include_directories(${DEPENDENCIES_openssl_DIR}/include)

