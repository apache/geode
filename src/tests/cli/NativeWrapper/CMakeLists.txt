cmake_minimum_required(VERSION 3.4)
project(NativeWrapper)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(NativeWrapper SHARED ${SOURCES})

target_link_libraries(NativeWrapper
  PUBLIC 
    gfclicache
    c++11
)

add_dependencies(NativeWrapper gfclicache)

include_directories(${CMAKE_SOURCE_DIR}/clicache/src)
set_target_properties(NativeWrapper PROPERTIES FOLDER test/cliTests)

set( DOT_NET_FRAMEWORK "C:/Program Files (x86)/Reference Assemblies/Microsoft/Framework/.NETFramework/v4.0")
set( ASM_PATH ${ASM_PATH} "/FU ${DOT_NET_FRAMEWORK}/System.dll")
set( ASM_PATH ${ASM_PATH} "/FU ${DOT_NET_FRAMEWORK}/System.Xml.dll")
target_compile_options(NativeWrapper PRIVATE ${ASM_PATH})
string(REPLACE "/EHsc" "/EHa" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
string(REPLACE "/RTC1" "" CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /clr /wd4947")
