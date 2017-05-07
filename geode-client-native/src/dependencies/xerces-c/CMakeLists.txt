cmake_minimum_required( VERSION 3.3 )
project( xerces-c )

set( ${PROJECT_NAME}_VERSION 3.1.2 )
set( ${PROJECT_NAME}_SHA265 743bd0a029bf8de56a587c270d97031e0099fe2b7142cef03e0da16e282655a0 )
set( ${PROJECT_NAME}_URL "http://archive.apache.org/dist/xerces/c/3/sources/xerces-c-${${PROJECT_NAME}_VERSION}.tar.gz" )
set( ${PROJECT_NAME}_LIB_VERSION 3.1 )

set ( ${PROJECT_NAME}_CFLAGS "${${PROJECT_NAME}_CFLAGS} ${CFLAGS_ARCH}" )
set ( ${PROJECT_NAME}_CXXFLAGS "${${PROJECT_NAME}_CXXFLAGS} ${CXXFLAGS_ARCH} ${CXXFLAGS_C++}" )

if (WIN32)
  if (64 EQUAL ${BUILD_BITS})
    set( _PLATFORM x64 )
  else()
    set( _PLATFORM win32 )
  endif()

  if (MSVC_VERSION GREATER 12)
    # Only have project files for VS12 and older
    set(MSVC_VERSION 12)
  endiF()

  set ( _DEBUG_OR_RELEASE $<$<CONFIG:Debug>:Debug>$<$<NOT:$<CONFIG:Debug>>:Release> )
  set ( _CONFIGURE_COMMAND "" )
  set ( _BUILD_COMMAND ${MSBUILD} projects\\Win32\\VC${MSVC_VERSION}\\xerces-all\\xerces-all.sln /m /p:Configuration=${_DEBUG_OR_RELEASE} /p:Platform=${_PLATFORM} )
  set ( _INSTALL_COMMAND ${CMAKE_COMMAND} -E copy Build/Win64/VC${MSVC_VERSION}/${_DEBUG_OR_RELEASE}/xerces-c_3_1$<$<CONFIG:Debug>:D>.dll <INSTALL_DIR>/bin/xerces-c_3_1$<$<CONFIG:Debug>:D>.dll
                 COMMAND ${CMAKE_COMMAND} -E copy Build/Win64/VC${MSVC_VERSION}/${_DEBUG_OR_RELEASE}/xerces-c_3$<$<CONFIG:Debug>:D>.lib <INSTALL_DIR>/lib/xerces-c_3$<$<CONFIG:Debug>:D>.lib
                 #TODO cleanup includes?
                 COMMAND ${CMAKE_COMMAND} -E copy_directory src <INSTALL_DIR>/include )
else()
  set ( _CONFIGURE_COMMAND ${CONFIGURE} $<$<CONFIG:Debug>:--with-debug> --with-pic "CFLAGS=${${PROJECT_NAME}_CFLAGS}" "CXXFLAGS=${${PROJECT_NAME}_CXXFLAGS}" )
  set ( _BUILD_COMMAND $(MAKE) all )
  set ( _INSTALL_COMMAND $(MAKE) install )
endif()

include(ExternalProject)

ExternalProject_Add( ${PROJECT_NAME}
   URL ${${PROJECT_NAME}_URL}
   URL_HASH SHA256=${${PROJECT_NAME}_SHA265}
   #TODO prefix - directory length too long for windows
   #PREFIX "/tmp/xerces-c"
   UPDATE_COMMAND ""
   BUILD_IN_SOURCE 1
   CONFIGURE_COMMAND "${_CONFIGURE_COMMAND}"
   BUILD_COMMAND "${_BUILD_COMMAND}"
   INSTALL_COMMAND "${_INSTALL_COMMAND}"
)

ExternalProject_Get_Property( ${PROJECT_NAME} SOURCE_DIR )
set( ${PROJECT_NAME}_SOURCE_DIR ${SOURCE_DIR} )
ExternalProject_Get_Property( ${PROJECT_NAME} INSTALL_DIR )
set( ${PROJECT_NAME}_INSTALL_DIR ${INSTALL_DIR} )

set( DEPENDENCIES_${PROJECT_NAME}_DIR ${${PROJECT_NAME}_INSTALL_DIR} PARENT_SCOPE)
set( DEPENDENCIES_${PROJECT_NAME}_VERSION ${${PROJECT_NAME}_VERSION} PARENT_SCOPE)
set( DEPENDENCIES_${PROJECT_NAME}_LIB_VERSION ${${PROJECT_NAME}_LIB_VERSION} PARENT_SCOPE)
if (${WIN32})
  #TODO find better way to handle import lib on windows
  set(CMAKE_SHARED_LIBRARY_SUFFIX _3$<$<CONFIG:Debug>:D>.lib)
endif()
set( ${PROJECT_NAME}_SHARED_LIB ${${PROJECT_NAME}_INSTALL_DIR}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}xerces-c${CMAKE_SHARED_LIBRARY_SUFFIX} PARENT_SCOPE)
