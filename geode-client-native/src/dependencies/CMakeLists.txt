cmake_minimum_required( VERSION 3.3 )
project( dependencies )

#TODO options?
set( USE_C++ 11 CACHE STRING "Use C++ standard X (11).")
set( BUILD_BITS 64 CACHE STRING "Build for X-bit (64)." )

set ( DEPENDENCIES
	libxml2
	xerces-c
	openssl
	ACE
	antlr
	sqlite
	doxygen
#	perl-Devel-GDB
)

if ( "" STREQUAL "${USE_C++}" )
  set (DEPENDENCIES STLport ${DEPENDENCIES})
endif()

include(CheckCCompilerFlag)
include(CheckCXXCompilerFlag)

set( CONFIGURE_FLAGS --prefix=<INSTALL_DIR> CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} )
set( CONFIGURE ./configure ${CONFIGURE_FLAGS} )

find_program( PATCH NAMES gpatch patch )
if ("PATCH-NOTFOUND" STREQUAL "${PATCH}")
  message( FATAL_ERROR "patch not found." )
endif()

# TODO windows why was windows finding bash?
find_program( BASH bash )

if (WIN32)
  find_program( MSBUILD msbuild )
  # TODO FindPerl
  find_program( PERL perl )

  # Determin the Visual Studio version to generate projects for.
  if (MSVC10)
    set(MSVC_VERSION 10)
  elseif(MSVC11)
    set(MSVC_VERSION 11)
  elseif(MSVC12)
    set(MSVC_VERSION 12)
  elseif(MSVC14)
    set(MSVC_VERSION 14)
  endIF()
  
  set(DEPENDENCIES ${DEPENDENCIES} sqlite-netFx)
endif()

# TODO autoconfig?
set( CFLAGS_ARCH_32 -m32 )
set( CFLAGS_ARCH_64 -m64 )

# TODO check_c*_compiler_flag don't support linker flags
#check_c_compiler_flag( ${CFLAGS_ARCH_${BUILD_BITS}} CFLAGS_ARCH_${BUILD_BITS}_ALLOWED )
#if (${CFLAGS_ARCH_${BUILD_BITS}_ALLOWED})
  set( CFLAGS_ARCH ${CFLAGS_ARCH_${BUILD_BITS}} )
#else()
#  message( FATAL_ERROR "${BUILD_BITS} bits flag ${CFLAGS_ARCH_${BUILD_BITS}} not supported.")
#endif()

set( CXXFLAGS_ARCH_32 -m32 )
set( CXXFLAGS_ARCH_64 -m64 )

#check_cxx_compiler_flag( ${CXXFLAGS_ARCH_${BUILD_BITS}} CXXFLAGS_ARCH_${BUILD_BITS}_ALLOWED )
#if (${CXXFLAGS_ARCH_${BUILD_BITS}_ALLOWED})
  set( CXXFLAGS_ARCH ${CXXFLAGS_ARCH_${BUILD_BITS}} )
#else()
#  message( FATAL_ERROR "${BUILD_BITS} bits flag ${CXXFLAGS_ARCH_${BUILD_BITS}} not supported.")
#endif()

set( CXXFLAGS_C++11 ${CMAKE_CXX11_STANDARD_COMPILE_OPTION} )
if (NOT "" STREQUAL "${CXXFLAGS_C++${USE_C++}}" )
		set( CXXFLAGS_C++ ${CXXFLAGS_C++${USE_C++}} )
endif()

#TODO solaris needs special flags https://docs.oracle.com/cd/E37069_01/html/E37075/bkamq.html#scrolltoc
set( CXXLDFLAGS_RUNTIME_C++11 -lstdc++ -lgcc_s -lCrunG3 -lc )
set( CXXLDFLAGS_RUNTIME ${CXXFLAGS_C++${USE_C++}} )

file( REMOVE ${CMAKE_CURRENT_BINARY_DIR}/dependencies.properties )
foreach(_D ${DEPENDENCIES})
	add_subdirectory( ${_D} )
	#ExternalProject_Get_Property( ${_D} INSTALL_DIR )
	#set( ${_D}_INSTALL_DIR ${INSTALL_DIR} )
	file( APPEND ${CMAKE_CURRENT_BINARY_DIR}/dependencies.properties "dependencies.${_D}.dir=${DEPENDENCIES_${_D}_DIR}\n" )
	set( DEPENDENCIES_${_D}_DIR ${DEPENDENCIES_${_D}_DIR} PARENT_SCOPE)
	set( DEPENDENCIES_${_D}_VERSION ${DEPENDENCIES_${_D}_VERSION} PARENT_SCOPE)
endforeach(_D)

set(ACE_STATIC_LIB ${ACE_STATIC_LIB} PARENT_SCOPE)
set(ACE_SSL_STATIC_LIB ${ACE_SSL_STATIC_LIB} PARENT_SCOPE)
set(libxml2_STATIC_LIB ${libxml2_STATIC_LIB} PARENT_SCOPE)
set(antlr_STATIC_LIB ${antlr_STATIC_LIB} PARENT_SCOPE)
set(xerces-c_SHARED_LIB ${xerces-c_SHARED_LIB} PARENT_SCOPE)
set(sqlite_SHARED_LIB ${sqlite_SHARED_LIB} PARENT_SCOPE)
set(openssl_CRYPTO_SHARED_LIB ${openssl_CRYPTO_SHARED_LIB} PARENT_SCOPE)
set(openssl_SSL_SHARED_LIB ${openssl_SSL_SHARED_LIB} PARENT_SCOPE)
set(sqlite-netFx_SHARED_LIB ${sqlite-netFx_SHARED_LIB} PARENT_SCOPE)


configure_file( ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty.properties.in ${CMAKE_CURRENT_BINARY_DIR}/thirdparty.properties @ONLY)
