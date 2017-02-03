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
project(nativeclient)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/../cmake)

set(BUILD_BITS 64 CACHE STRING "Build for 64 (default) or 32 bit.")

set(PRODUCT_VENDOR "Apache" CACHE STRING "Product vendor")
set(PRODUCT_VENDOR_NAME "The Apache Software Foundation" CACHE STRING "Product vendor full legal name")
set(PRODUCT_NAME "Geode Native" CACHE STRING "Product name")
set(PRODUCT_VERSION "0.0.42-build.0" CACHE STRING "Product version")
set(PRODUCT_PACKAGE_NAME "apache-geode-native" CACHE STRING "Product package name")

set(PRODUCT_BITS "${BUILD_BITS}bit")

if(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
  if (CMAKE_SYSTEM_PROCESSOR STREQUAL "sparc")
    set(PRODUCT_SYSTEM_NAME "solaris-sparc")
  else()
    set(PRODUCT_SYSTEM_NAME "solaris-x86")
  endif()
else()
  set(PRODUCT_SYSTEM_NAME "${CMAKE_SYSTEM_NAME}" CACHE STRING "Product built for system name")
endif()

string(REGEX REPLACE "-build" "" PRODUCT_VERSION_DOTTED ${PRODUCT_VERSION})
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" PRODUCT_VERSION_LONG ${PRODUCT_VERSION})
string(REGEX MATCH "^[0-9]+\\.[0-9]+" PRODUCT_VERSION_SHORT ${PRODUCT_VERSION})
string(REGEX REPLACE "\\." ";" PRODUCT_VERSION_LIST ${PRODUCT_VERSION_DOTTED})
list(LENGTH PRODUCT_VERSION_LIST _length)
if (_length LESS 4)
  foreach(_index RANGE ${_length} 3)
    list(APPEND PRODUCT_VERSION_LIST 0)
  endforeach()
endif()
list(GET PRODUCT_VERSION_LIST 0 PRODUCT_VERSION_MAJOR)
list(GET PRODUCT_VERSION_LIST 1 PRODUCT_VERSION_MINOR)
list(GET PRODUCT_VERSION_LIST 2 PRODUCT_VERSION_PATCH)
list(GET PRODUCT_VERSION_LIST 3 PRODUCT_VERSION_BUILD)

# Please note that attempts to set CMAKE_INSTALL_PREFIX to a *ROOTED*
# path here will fail due to the design of CMake according to its
# development team. Setting it to a relative path appears to work.
# To override the install location, CMAKE_INSTALL_PREFIX must be
# specified at the time of generation, e.g.:
# $ cmake -G Xcode -DCMAKE_INSTALL_PREFIX=/my/favorite/location ../src
set(CMAKE_INSTALL_PREFIX "nativeclient" CACHE STRING "Install prefix")

set(CMAKE_CONFIGURATION_TYPES Debug Release)
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

# TODO this doesn't seem to have effect
set(CMAKE_ECLIPSE_VERSION Mars)

#TODO this check is failing to fail properly on solaris with sun CC 5.10
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CPACK_PACKAGE_VERSION_MAJOR ${PRODUCT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PRODUCT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PRODUCT_VERSION_PATCH})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "${PRODUCT_VENDOR} ${PRODUCT_NAME}")
set(CPACK_PACKAGE_VENDOR "${PRODUCT_VENDOR}")
set(CPACK_PACKAGE_NAME "${PRODUCT_PACKAGE_NAME}")
set(CPACK_SYSTEM_NAME "${PRODUCT_SYSTEM_NAME}-${PRODUCT_BITS}")
set(CPACK_PACKAGE_FILE_NAME ${CPACK_PACKAGE_NAME}-${PRODUCT_VERSION}-${CPACK_SYSTEM_NAME})
set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY 0)
set(CPACK_PACKAGING_INSTALL_PREFIX "/${CPACK_PACKAGE_NAME}")
#set(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/README")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/../dist/LICENSE")
set(CPACK_PACKAGE_INSTALL_DIRECTORY "${CPACK_PACKAGE_NAME}")
set(CPACK_GENERATOR TGZ;ZIP)

option(ENABLE_CLANG_TIDY "Enable clang-tidy checks." false)
if(ENABLE_CLANG_TIDY)
  find_program(CLANG_TIDY "clang-tidy")
  if(CLANG_TIDY)
    set(CMAKE_CXX_CLANG_TIDY ${CLANG_TIDY})
    option(ENABLE_CLANG_TIDY_FIX "Enable clang-tidy fix." false)
    if(ENABLE_CLANG_TIDY)
      set(CMAKE_CXX_CLANG_TIDY ${CMAKE_CXX_CLANG_TIDY} -fix -fix-errors -header-filter=${CMAKE_CURRENT_SOURCE_DIR})
    endif()
  else()
    message(SEND_ERROR "clang-tidy enabled but not found.")
  endif()
endif()

file(GLOB_RECURSE ALL_CXX_SOURCE_FILES *.[chi]pp *.[chi]xx *.cc *.hh *.ii *.[CHI])
find_program(CLANG_FORMAT "clang-format")
if(CLANG_FORMAT)
  add_custom_target(format)
  set_target_properties(format PROPERTIES EXCLUDE_FROM_ALL TRUE EXCLUDE_FROM_DEFAULT_BUILD TRUE)
  
  foreach(_file ${ALL_CXX_SOURCE_FILES})
    add_custom_command(TARGET format PRE_BUILD COMMAND ${CLANG_FORMAT} -i -style=file -fallback-style=Google ${_file})
  endforeach()
  
  #add_custom_target(format COMMAND ${CLANG_FORMAT} -i -style=file -fallback-style=Google ${ALL_CXX_SOURCE_FILES})
  #set_target_properties(format PROPERTIES EXCLUDE_FROM_ALL TRUE EXCLUDE_FROM_DEFAULT_BUILD TRUE)
endif()

include(CPack)
include(CheckCXXCompilerFlag)
include(CheckCCompilerFlag)

set(CMAKE_REQUIRED_LIBRARIES -m64)
check_c_compiler_flag(-m64 CFLAGS_M64_ALLOWED)
check_cxx_compiler_flag(-m64 CXXFLAGS_M64_ALLOWED)
set(CMAKE_REQUIRED_LIBRARIES)

check_c_compiler_flag(-mt CFLAGS_mt_ALLOWED)

if (CFLAGS_M64_ALLOWED AND CXXFLAGS_M64_ALLOWED)
  set(CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS} -m64)
  add_compile_options(-m64)
#TODO cmake find better way to set linker flags
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -m64")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -m64")
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -m64")
endif()

if (CFLAGS_mt_ALLOWED)
  set(CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS} -mt)
  add_compile_options(-mt)
endif()

#TODO remove this debugging for NMake
set(CMAKE_VERBOSE_MAKEFILE 0)

add_library(c++11 INTERFACE)


if(CMAKE_CXX_COMPILER_ID STREQUAL "SunPro")
  # Force linker to error on undefined symbols in shared libraries
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -z defs")
  # TODO cmake find a better way to set runtime libraries
  # C++11 requires these libraries, treat -std=c++11 as library
  #TODO look into CMAKE_CXX_STANDARD_LIBRARIES
  target_link_libraries(c++11 INTERFACE -std=c++11 stdc++ gcc_s CrunG3 c)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,-z,defs")
endif()
if(MSVC)
    # TODO error on warnings
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4996")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /MP")
  set(BUILD_CLI 1)
  set(STRONG_NAME_KEY "" CACHE FILEPATH "Strong Name Key File")
  if(EXISTS "${STRONG_NAME_KEY}")
    set(SHARED_LINKER_FLAGS_STRONG_KEY "/keyfile:${STRONG_NAME_KEY}")
    execute_process(COMMAND sn -p ${STRONG_NAME_KEY} ${CMAKE_CURRENT_BINARY_DIR}/public.snk)
    execute_process(COMMAND sn -tp ${CMAKE_CURRENT_BINARY_DIR}/public.snk OUTPUT_VARIABLE STRONG_NAME_PUBLIC_KEY)
    string(REPLACE "\n" "" STRONG_NAME_PUBLIC_KEY ${STRONG_NAME_PUBLIC_KEY})
    string(REGEX REPLACE ".*sha1\\):([a-f0-9]+).*" "\\1" STRONG_NAME_PUBLIC_KEY ${STRONG_NAME_PUBLIC_KEY})
  endif()
endif()

find_package(Java 1.8.0.60 REQUIRED COMPONENTS Development)

if ("${CMAKE_AR}" STREQUAL "CMAKE_AR-NOTFOUND")
  message(FATAL_ERROR "Utility ar not found.")
endif()

if (WIN32 OR ${CMAKE_GENERATOR} STREQUAL "Xcode")
  set(CMAKE_USES_BUILDTYPE TRUE)
  set ( _DEBUG_OR_RELEASE $<$<CONFIG:Debug>:Debug>$<$<NOT:$<CONFIG:Debug>>:Release>)
else()
  set(CMAKE_USES_BUILDTYPE FALSE)
  set ( _DEBUG_OR_RELEASE )
endif()

# Since RPATH embeds path information into the executables that
# may not be valid in all execution contexts, do not use RPATH.
set(CMAKE_SKIP_RPATH TRUE)
set(CMAKE_MACOSX_RPATH FALSE)

# Default to only showing output on failure for unit tests but allow
# overriding with the CTEST_UNITTEST_VERBOSITY environment variable.
set(CTEST_UNITTEST_VERBOSITY --output-on-failure)
if(DEFINED ENV{CTEST_UNITTEST_VERBOSITY})
    set(CTEST_UNITTEST_VERBOSITY $ENV{CTEST_UNITTEST_VERBOSITY})
endif()

find_package(Geode 1.0 REQUIRED)

add_custom_target(client-libraries)

add_custom_target(unit-tests)
add_custom_target(run-unit-tests)
add_dependencies(run-unit-tests unit-tests)
set_target_properties(run-unit-tests PROPERTIES
    EXCLUDE_FROM_ALL TRUE
    EXCLUDE_FROM_DEFAULT_BUILD TRUE
)

add_custom_target(integration-tests)
add_custom_target(run-integration-tests)
add_dependencies(run-integration-tests integration-tests)
set_target_properties(run-integration-tests PROPERTIES
    EXCLUDE_FROM_ALL TRUE
    EXCLUDE_FROM_DEFAULT_BUILD TRUE
)

add_subdirectory(dependencies)
add_subdirectory(cppcache)
add_subdirectory(cryptoimpl)
add_subdirectory(dhimpl)
add_subdirectory(sqliteimpl)
add_subdirectory(gfcpp)
add_subdirectory(pdxautoserializer)
add_subdirectory(tests)
add_subdirectory(templates/security)
add_subdirectory(docs)
if (${BUILD_CLI})
  add_subdirectory(clicache)
  add_subdirectory(plugins/SQLiteCLI)
endif()

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/quickstart/ DESTINATION SampleCode/quickstart)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/cppcache/integration-test/keystore/ DESTINATION SampleCode/quickstart/keystore)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/xsds/ DESTINATION xsds)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/defaultSystem/ DESTINATION defaultSystem)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../dist/ DESTINATION .)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/templates/ DESTINATION templates
  PATTERN "templates/security/CMakeLists.txt" EXCLUDE
  PATTERN "templates/security/CMakeLists.txt.forInstall" EXCLUDE)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/templates/security/CMakeLists.txt.forInstall RENAME CMakeLists.txt DESTINATION templates/security)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/tests/javaobject/javaobject.jar DESTINATION SampleCode/quickstart/lib)
