cmake_minimum_required( VERSION 3.3 )
project( xerces-c )

set( ${PROJECT_NAME}_VERSION 3.1.4 )
set( ${PROJECT_NAME}_SHA265 c98eedac4cf8a73b09366ad349cb3ef30640e7a3089d360d40a3dde93f66ecf6 )
set( ${PROJECT_NAME}_URL "http://archive.apache.org/dist/xerces/c/3/sources/xerces-c-${${PROJECT_NAME}_VERSION}.tar.gz" )
set( ${PROJECT_NAME}_EXTERN ${PROJECT_NAME}-extern )

set ( ${PROJECT_NAME}_CFLAGS "${${PROJECT_NAME}_CFLAGS} ${CFLAGS_ARCH}" )
set ( ${PROJECT_NAME}_CXXFLAGS "${${PROJECT_NAME}_CXXFLAGS} ${CXXFLAGS_ARCH} ${CXXFLAGS_C++}" )

if (WIN32)
  if (64 EQUAL ${BUILD_BITS})
    set( _PLATFORM x64 )
    set( _BUILD_DIR Build/Win64/VC${MSVC_VERSION}/${_DEBUG_OR_RELEASE} )
  else()
    set( _PLATFORM win32 )
    set( _BUILD_DIR Build/Win32/VC${MSVC_VERSION}/${_DEBUG_OR_RELEASE} )
  endif()

  if (MSVC_VERSION GREATER 12)
    # Only have project files for VS12 and older
    set(MSVC_VERSION 12)
  endiF()

  set ( _CONFIGURE_COMMAND "" )
  set ( _BUILD_COMMAND ${MSBUILD} projects\\Win32\\VC${MSVC_VERSION}\\xerces-all\\xerces-all.sln /m /p:Configuration=${_DEBUG_OR_RELEASE} /p:Platform=${_PLATFORM} )
  set ( _INSTALL_COMMAND ${CMAKE_COMMAND} -E copy ${_BUILD_DIR}/xerces-c_3_1$<$<CONFIG:Debug>:D>.dll <INSTALL_DIR>/bin/xerces-c_3_1$<$<CONFIG:Debug>:D>.dll
                 COMMAND ${CMAKE_COMMAND} -E copy ${_BUILD_DIR}/xerces-c_3$<$<CONFIG:Debug>:D>.lib <INSTALL_DIR>/lib/xerces-c_3$<$<CONFIG:Debug>:D>.lib
                 #TODO cleanup includes?
                 COMMAND ${CMAKE_COMMAND} -E copy_directory src <INSTALL_DIR>/include )
else()
  set ( _CONFIGURE_COMMAND ${CONFIGURE} $<$<CONFIG:Debug>:--with-debug> --with-pic "CFLAGS=${${PROJECT_NAME}_CFLAGS}" "CXXFLAGS=${${PROJECT_NAME}_CXXFLAGS}" "MAKE=$(MAKE)" )
  set ( _BUILD_COMMAND $(MAKE) all )
  set ( _INSTALL_COMMAND $(MAKE) install )
endif()

include(ExternalProject)

ExternalProject_Add( ${${PROJECT_NAME}_EXTERN}
   URL ${${PROJECT_NAME}_URL}
   URL_HASH SHA256=${${PROJECT_NAME}_SHA265}
   #TODO prefix - directory length too long for windows
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
  #TODO find better way to handle import lib on windows
  set(CMAKE_SHARED_LIBRARY_SUFFIX _3$<$<CONFIG:Debug>:D>.lib)
endif()

add_library(${PROJECT_NAME} INTERFACE)
target_include_directories(${PROJECT_NAME} INTERFACE
  $<BUILD_INTERFACE:${${PROJECT_NAME}_INSTALL_DIR}/include>
)
target_link_libraries(${PROJECT_NAME} INTERFACE
    ${${PROJECT_NAME}_INSTALL_DIR}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}xerces-c${CMAKE_SHARED_LIBRARY_SUFFIX}
)
add_dependencies(${PROJECT_NAME} ${${PROJECT_NAME}_EXTERN})

set_target_properties(${PROJECT_NAME} PROPERTIES INTERFACE_LIBRARY_DIR ${${PROJECT_NAME}_INSTALL_DIR}/lib)
set_target_properties(${PROJECT_NAME} PROPERTIES INTERFACE_RUNTIME_DIR ${${PROJECT_NAME}_INSTALL_DIR}/bin)

