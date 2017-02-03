# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 3.4)
project(cppcache)

file(GLOB_RECURSE SOURCES "*.cpp")

file(GLOB_RECURSE PRIVATE_HEADERS "*.hpp")
set_source_files_properties(${PRIVATE_HEADERS} PROPERTIES HEADER_FILE_ONLY TRUE)

file(GLOB_RECURSE PUBLIC_HEADERS "${CMAKE_CURRENT_SOURCE_DIR}/../include/*")
set_source_files_properties(${PUBLIC_HEADERS} PROPERTIES HEADER_FILE_ONLY TRUE)

set(RESOURCES apache-geode.rc)

find_package(Git 1.7 REQUIRED)

string(REPLACE ";" "," PRODUCT_VERSION_RC "${PRODUCT_VERSION_LIST}")
list(APPEND CONFIGURE_IN_FILES ${CMAKE_CURRENT_SOURCE_DIR}/version.cmake.in)
list(APPEND CONFIGURE_OUT_FILES ${CMAKE_CURRENT_BINARY_DIR}/version.cmake)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/version.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/version.cmake @ONLY)

list(APPEND CONFIGURE_OUT_FILES ${CMAKE_CURRENT_BINARY_DIR}/version.h)
add_custom_command(OUTPUT version.h
    COMMAND ${CMAKE_COMMAND} -P version.cmake
    DEPENDS ${CMAKE_SOURCE_DIR}/../.git/index ${CMAKE_CURRENT_BINARY_DIR}/version.cmake
    MAIN_DEPENDENCY ${CMAKE_SOURCE_DIR}/../.git/index
)
# adds named target for version.h to avoid parallel generation under windows
add_custom_target(version-header DEPENDS version.h)


# TODO checks
include(CheckIncludeFiles)
check_include_files("sys/param.h;sys/mount.h" HAVE_SYS_MOUNT_H)

include(CheckSymbolExists)
check_symbol_exists(SIGSTKFLT "signal.h" HAVE_SIGSTKFLT)
check_symbol_exists(SIGPWR "signal.h" HAVE_SIGPWR)

include(CheckCXXSymbolExists)
#TODO how can we do this, ACE not built yet
#check_cxx_symbol_exists(ACE::ACE_Select_Reactor "ace/config.h;ace/Select_Reactor.h" HAVE_ACE_Select_Reactor)
#check_cxx_symbol_exists(ACE::ACE_Dev_Poll_Reactor "ace/config.h;ace/Dev_Poll_Reactor.h" HAVE_ACE_Dev_Poll_Reactor)

#TODO cmake collides with gf_globals.hpp
if ("SunOS" STREQUAL ${CMAKE_SYSTEM_NAME})
  set( _SOLARIS 1 )
elseif ("Linux" STREQUAL ${CMAKE_SYSTEM_NAME} )
  set( _LINUX 1 )
elseif ("Darwin" STREQUAL ${CMAKE_SYSTEM_NAME} )
  set( _MACOSX 1 )
elseif ("Windows" STREQUAL ${CMAKE_SYSTEM_NAME} )
  set( _WIN32 1 )
endif()

list(APPEND CONFIGURE_IN_FILES ${CMAKE_CURRENT_SOURCE_DIR}/config.h.in)
list(APPEND CONFIGURE_OUT_FILES ${CMAKE_CURRENT_BINARY_DIR}/config.h)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

#set_source_files_properties(${CONFIGURE_IN_FILES} PROPERTIES LANGUAGE NONE)
set_source_files_properties(${CONFIGURE_OUT_FILES} PROPERTIES GENERATED TRUE)

set(SOURCES_ALL ${SOURCES} ${PRIVATE_HEADERS} ${PUBLIC_HEADERS} ${CONFIGURE_IN_FILES} ${CONFIGURE_OUT_FILES})


add_library(_apache-geode INTERFACE)

if(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
  target_link_libraries(_apache-geode INTERFACE
    demangle
    kstat
  )
  if (CMAKE_SYSTEM_PROCESSOR STREQUAL "sparc")
    set(CMAKE_ASM_COMPILER fbe)
    set(CMAKE_ASM_SOURCE_FILE_EXTENSIONS asm)
    set(CMAKE_ASM_COMPILE_OBJECT "<CMAKE_ASM_COMPILER> -m64 -K PIC -L -s -P -o <OBJECT> <SOURCE>")
    enable_language(ASM)
    add_library(gfcppasm OBJECT impl/hostsolaris.asm)
    set(SOURCES ${SOURCES} $<TARGET_OBJECTS:gfcppasm>)
  endif()
endif()
if (${WIN32})
  target_link_libraries(_apache-geode INTERFACE
      Dbghelp
  )
endif()

target_link_libraries(_apache-geode INTERFACE
  ACE
  libxml2
)
target_compile_definitions(_apache-geode INTERFACE
    # TODO replace BUILD_CPPCACHE with built-in _DLL
    $<BUILD_INTERFACE:BUILD_CPPCACHE>
)
target_include_directories(_apache-geode INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
)
add_dependencies(_apache-geode version-header)

add_library(apache-geode SHARED ${SOURCES_ALL} ${RESOURCES})
target_link_libraries(apache-geode
  PRIVATE
    _apache-geode
  PUBLIC
    c++11
)
target_include_directories(apache-geode
  PUBLIC
    $<TARGET_PROPERTY:_apache-geode,INTERFACE_INCLUDE_DIRECTORIES>
)
add_dependencies(client-libraries apache-geode)
set_target_properties(apache-geode PROPERTIES PUBLIC_HEADER "${PUBLIC_HEADERS}")

add_library(apache-geode-static STATIC ${SOURCES_ALL})
target_link_libraries(apache-geode-static
  PUBLIC
    _apache-geode
)
add_dependencies(client-libraries apache-geode-static)

install(TARGETS apache-geode
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
  PUBLIC_HEADER DESTINATION include/gfcpp
)

# For Visual Studio organization
source_group("Header Files" REGULAR_EXPRESSION "\.(hpp|inl)$")
source_group("Configure In Files" FILES ${CONFIGURE_IN_FILES})
source_group("Configure Out Files" FILES ${CONFIGURE_OUT_FILES})
