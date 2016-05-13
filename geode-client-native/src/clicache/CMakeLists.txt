cmake_minimum_required(VERSION 3.4)
project(clicache)

file(GLOB_RECURSE SOURCES "*.cpp")

#set(PLATFORM_LIBRARIES ${PLATFORM_LIBRARIES} Dbghelp)
add_library(gfclicache SHARED ${SOURCES})

#TODO get external project library names
target_link_libraries(gfclicache
  PUBLIC
  PRIVATE
    gfcppcache-static
    psapi
    ${ACE_STATIC_LIB}
    ${libxml2_STATIC_LIB}
)

add_dependencies(gfclicache libxml2 ACE)

#TODO cmake DEBUG add_definitions(-DACE_NLOGGING -DACE_NDEBUG )
#TODO move to config.h when not building shared ace
add_definitions(-D__ACE_INLINE__)
add_definitions(-DACE_AS_STATIC_LIBS)

string(REPLACE "/EHsc" "/EHa" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
string(REPLACE "/RTC1" "" CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /clr /wd4947 /doc")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /keyfile:${CMAKE_SOURCE_DIR}/../release/keys/gemfire.snk")

set_target_properties(gfclicache PROPERTIES
  OUTPUT_NAME Gemstone.Gemfire.Cache
  VS_DOTNET_TARGET_FRAMEWORK_VERSION "v4.5.1"
  VS_DOTNET_REFERENCES "System;System.Xml")

#TODO move cppcache headers?
include_directories(${CMAKE_BINARY_DIR}/cppcache)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/cppcache)
include_directories(${DEPENDENCIES_ACE_DIR}/include)
include_directories(${DEPENDENCIES_libxml2_DIR}/include/libxml2)

add_subdirectory(templates)
