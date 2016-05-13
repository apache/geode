cmake_minimum_required( VERSION 3.3 )
project( openssl C )

set( ${PROJECT_NAME}_VERSION 1.0.1s )
set( ${PROJECT_NAME}_SHA265 e7e81d82f3cd538ab0cdba494006d44aab9dd96b7f6233ce9971fb7c7916d511 )
set( ${PROJECT_NAME}_URL "http://www.openssl.org/source/openssl-${${PROJECT_NAME}_VERSION}.tar.gz" )

set ( ${PROJECT_NAME}_CFLAGS "${${PROJECT_NAME}_CFLAGS} ${CFLAGS_ARCH}" )

if ("SunOS" STREQUAL ${CMAKE_SYSTEM_NAME})
  # No debug for Solaris without patching Configure
  if ("i386" STREQUAL ${CMAKE_SYSTEM_PROCESSOR})
    if (64 EQUAL ${BUILD_BITS})
      set( openssl_PLATFORM solaris64-x86_64-cc )
    else()
      set( openssl_PLATFORM solaris-x86-cc )
    endif()
  elseif ("sparc" STREQUAL ${CMAKE_SYSTEM_PROCESSOR})
    if (64 EQUAL ${BUILD_BITS})
      set( openssl_PLATFORM solaris64-sparcv9-cc )
    else()
      set( openssl_PLATFORM solaris-sparcv9-cc )
    endif()
  endif()
elseif ("Linux" STREQUAL ${CMAKE_SYSTEM_NAME})
  if (64 EQUAL ${BUILD_BITS})
    set( openssl_PLATFORM $<$<CONFIG:Debug>:debug->linux-x86_64 )
  else()
    set( openssl_PLATFORM $<$<CONFIG:Debug>:debug->linux-elf )
    set( openssl_CONFIGURE_FLAGS ${openssl_CONFIGURE_FLAGS} -m32 )
  endif()
elseif ("Darwin" STREQUAL ${CMAKE_SYSTEM_NAME})
  # No debug for OS X without patching Configure
  set( openssl_PLATFORM darwin64-x86_64-cc )
elseif ("Windows" STREQUAL ${CMAKE_SYSTEM_NAME})
  if (64 EQUAL ${BUILD_BITS})
    set( openssl_PLATFORM $<$<CONFIG:Debug>:debug->VC-WIN64A )
  else()
    set( openssl_PLATFORM $<$<CONFIG:Debug>:debug->VC-WIN32 )
  endif()
endif()

if (NOT DEFINED openssl_PLATFORM)
  message( FATAL_ERROR "openssl_PLATFORM unset for ${CMAKE_SYSTEM_NAME}/${CMAKE_SYSTEM_PROCESSOR}" )
endif()

if (${WIN32})
  # Keeps separate release/debug objects in build script
  set ( _CONFIGURE_COMMAND ${PERL} Configure --prefix=<INSTALL_DIR>/$<CONFIG> ${openssl_CONFIGURE_FLAGS} ${openssl_PLATFORM} COMMAND cmd /c ms\\do_win64a)
  set ( _BUILD_COMMAND nmake /f ms\\ntdll.mak )
  set ( _INSTALL_COMMAND nmake /f ms\\ntdll.mak install )
else()
   # TODO Configure trips up without MAKE
  set ( _CONFIGURE_COMMAND MAKE=$(MAKE) ./Configure threads zlib shared --prefix=<INSTALL_DIR>/$<CONFIG> ${openssl_CONFIGURE_FLAGS} ${openssl_PLATFORM} )
  set ( _BUILD_COMMAND $(MAKE) all )
  set ( _INSTALL_COMMAND $(MAKE) install_sw )
endif()

include(ExternalProject)

ExternalProject_Add( ${PROJECT_NAME}
   URL ${${PROJECT_NAME}_URL}
   URL_HASH SHA256=${${PROJECT_NAME}_SHA265}
   UPDATE_COMMAND ""
   BUILD_IN_SOURCE 1
   CONFIGURE_COMMAND "${_CONFIGURE_COMMAND}"
   BUILD_COMMAND "${_BUILD_COMMAND}"
   INSTALL_COMMAND "${_INSTALL_COMMAND}"
# TODO   TEST_COMMAND $(MAKE) test
)

ExternalProject_Get_Property( ${PROJECT_NAME} SOURCE_DIR )
set( ${PROJECT_NAME}_SOURCE_DIR ${SOURCE_DIR} )
ExternalProject_Get_Property( ${PROJECT_NAME} INSTALL_DIR )
set( ${PROJECT_NAME}_INSTALL_DIR ${INSTALL_DIR}/$<CONFIG> )

set( DEPENDENCIES_${PROJECT_NAME}_DIR ${${PROJECT_NAME}_INSTALL_DIR} PARENT_SCOPE)
set( DEPENDENCIES_${PROJECT_NAME}_VERSION ${${PROJECT_NAME}_VERSION} PARENT_SCOPE)

if (${WIN32})
  set( CRYPTO_NAME libeay32 )
  set( SSL_NAME ssleay32 )
else()
  set( CRYPTO_NAME crypto )
  set( SSL_NAME ssl )
  set( CMAKE_LINK_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif()
set( ${PROJECT_NAME}_CRYPTO_SHARED_LIB ${${PROJECT_NAME}_INSTALL_DIR}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}${CRYPTO_NAME}${CMAKE_LINK_LIBRARY_SUFFIX} PARENT_SCOPE)
set( ${PROJECT_NAME}_SSL_SHARED_LIB ${${PROJECT_NAME}_INSTALL_DIR}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}${SSL_NAME}${CMAKE_LINK_LIBRARY_SUFFIX} PARENT_SCOPE)

ExternalProject_Add_Step( ${PROJECT_NAME} patches
    BYPRODUCTS ${${PROJECT_NAME}_SOURCE_DIR}/Configure
    ALWAYS 0
    DEPENDEES download
    DEPENDERS patch
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/patches
    WORKING_DIRECTORY ${${PROJECT_NAME}_SOURCE_DIR}
    COMMAND ${PATCH} -u -N -p1 < ${CMAKE_CURRENT_SOURCE_DIR}/patches
)
