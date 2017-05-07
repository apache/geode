cmake_minimum_required(VERSION 3.4)
project(nativeclient)

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

if(CMAKE_CXX_COMPILER_ID STREQUAL "SunPro")
  # Force linker to error on undefined symbols in shared libraries
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -z defs")
  # TODO cmake find a better way to set runtime libraries
  # C++11 requires these libraries, treat -std=c++11 as library
  #TODO look into CMAKE_CXX_STANDARD_LIBRARIES
  set(RUNTIME_LIBRARIES ${RUNTIME_LIBRARIES} -std=c++11 stdc++ gcc_s CrunG3 c)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,-z,defs")
endif()
if(MSVC)
    # TODO error on warnings
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4996")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /MP")
  set(BUILD_CLI 1)
endif()

find_package(Java 1.8.0.60 REQUIRED COMPONENTS Development)

if ("${CMAKE_AR}" STREQUAL "CMAKE_AR-NOTFOUND")
  message(FATAL_ERROR "Utility ar not found.")
endif()

set(GEMFIRE_HOME "" CACHE PATH "Path to GemFire production installation.")

add_subdirectory(dependencies)
add_subdirectory(cppcache)
add_subdirectory(cryptoimpl)
add_subdirectory(dhimpl)
add_subdirectory(sqliteimpl)
add_subdirectory(gfcpp)
add_subdirectory(pdxautoserializer)
add_subdirectory(tests)
add_subdirectory(templates/security)


if (${BUILD_CLI})
  add_subdirectory(clicache)
  add_subdirectory(plugins/SQLiteCLI)
endif()
