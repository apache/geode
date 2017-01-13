cmake_minimum_required( VERSION 3.3 )
project( libxml2 )

set( ${PROJECT_NAME}_VERSION 2.9.4 )
set( ${PROJECT_NAME}_SHA265 ffb911191e509b966deb55de705387f14156e1a56b21824357cdf0053233633c )
set( ${PROJECT_NAME}_URL "http://xmlsoft.org/sources/libxml2-${${PROJECT_NAME}_VERSION}.tar.gz" )
set( ${PROJECT_NAME}_EXTERN ${PROJECT_NAME}-extern )

set( ${PROJECT_NAME}_CFLAGS "${${PROJECT_NAME}_CFLAGS} ${CFLAGS_ARCH}" )

if (${WIN32})
  set (_MAKE_FLAGS ${_MAKE_FLAGS} "CPP=cl.exe /MP /EP" "CC=cl.exe /MP" )
  set (_MAKE_FLAGS ${_MAKE_FLAGS}
                          DEBUG=$<STREQUAL:$<CONFIG>,Debug>
                          CRUNTIME=/MD$<$<CONFIG:Debug>:d>
                          XML_SO=libxml2$<$<CONFIG:Debug>:d>.dll
                          XML_IMP=libxml2$<$<CONFIG:Debug>:d>.lib
                          XML_A=libxml2_a$<$<CONFIG:Debug>:d>.lib
                          XML_A_DLL=libxml2_a_dll$<$<CONFIG:Debug>:d>.lib
                          XML_INTDIR=int$<$<CONFIG:Debug>:d>.msvc
                          XML_INTDIR_A=int$<$<CONFIG:Debug>:d>.a.msvc
                          XML_INTDIR_A_DLL=int$<$<CONFIG:Debug>:d>.a.dll.msvc
                          UTILS_INTDIR=int$<$<CONFIG:Debug>:d>.utils.msvc )

  # http://hostagebrain.blogspot.com/2015/06/building-libxml2-without-dependencies.html
  set ( _CONFIGURE_COMMAND ${CMAKE_COMMAND} -E chdir win32 cscript configure.js iconv=no compiler=msvc cruntime=/MD$<$<CONFIG:Debug>:d> debug=$<$<CONFIG:Debug>:yes>$<$<NOT:$<CONFIG:Debug>>:no> prefix=..\\..\\.. static=yes )
  set ( _BUILD_COMMAND ${CMAKE_COMMAND} -E chdir win32 nmake -f Makefile.msvc all ${_MAKE_FLAGS} )
  set ( _INSTALL_COMMAND ${CMAKE_COMMAND} -E chdir win32 nmake -f Makefile.msvc install ${_MAKE_FLAGS} )
else()
  set ( _CONFIGURE_COMMAND ${CONFIGURE} $<$<CONFIG:Debug>:--with-debug> --with-pic --enable-static --without-iconv --without-python --without-lzma CFLAGS=${${PROJECT_NAME}_CFLAGS} "MAKE=$(MAKE)")
  set ( _BUILD_COMMAND $(MAKE) all )
  set ( _INSTALL_COMMAND $(MAKE) install )
endif()

include(ExternalProject)

ExternalProject_Add( ${${PROJECT_NAME}_EXTERN}
   URL ${${PROJECT_NAME}_URL}
   URL_HASH SHA256=${${PROJECT_NAME}_SHA265}
   UPDATE_COMMAND ""
   BUILD_IN_SOURCE 1
   CONFIGURE_COMMAND "${_CONFIGURE_COMMAND}"
   BUILD_COMMAND "${_BUILD_COMMAND}"
   INSTALL_COMMAND "${_INSTALL_COMMAND}"
)

ExternalProject_Get_Property( ${${PROJECT_NAME}_EXTERN} SOURCE_DIR )
set( ${PROJECT_NAME}_SOURCE_DIR ${SOURCE_DIR} )
ExternalProject_Get_Property( ${${PROJECT_NAME}_EXTERN} INSTALL_DIR )
set( ${PROJECT_NAME}_INSTALL_DIR ${INSTALL_DIR} )
set( DEPENDENCIES_${PROJECT_NAME}_DIR ${${PROJECT_NAME}_INSTALL_DIR} PARENT_SCOPE)

if (${WIN32})
  set(CMAKE_STATIC_LIBRARY_PREFIX lib)
  set(CMAKE_STATIC_LIBRARY_SUFFIX _a$<$<CONFIG:Debug>:d>.lib)
endif()

add_library(${PROJECT_NAME} INTERFACE)
target_include_directories(${PROJECT_NAME} INTERFACE
  $<BUILD_INTERFACE:${${PROJECT_NAME}_INSTALL_DIR}/include/libxml2>
)
target_link_libraries(${PROJECT_NAME} INTERFACE
    ${${PROJECT_NAME}_INSTALL_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}xml2${CMAKE_STATIC_LIBRARY_SUFFIX}
)
add_dependencies(${PROJECT_NAME} ${${PROJECT_NAME}_EXTERN})
