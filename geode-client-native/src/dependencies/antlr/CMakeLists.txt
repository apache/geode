cmake_minimum_required( VERSION 3.3 )
project( antlr )

set( ${PROJECT_NAME}_VERSION 2.7.7 )
set( ${PROJECT_NAME}_SHA265 853aeb021aef7586bda29e74a6b03006bcb565a755c86b66032d8ec31b67dbb9 )
set( ${PROJECT_NAME}_URL "http://www.antlr2.org/download/antlr-${${PROJECT_NAME}_VERSION}.tar.gz" )

string ( STRIP "${${PROJECT_NAME}_CFLAGS} ${CFLAGS_ARCH}" ${PROJECT_NAME}_CFLAGS )
string ( STRIP "${${PROJECT_NAME}_CXXFLAGS} ${CXXFLAGS_ARCH} ${CXXFLAGS_C++}" ${PROJECT_NAME}_CXXFLAGS )

# antlr configure doesn't save CFLAGS/CXXFLAGS
# TODO use CMake env command
set ( ${PROJECT_NAME}_MAKE $(MAKE) "CFLAGS=${${PROJECT_NAME}_CFLAGS}" "CXXFLAGS=${${PROJECT_NAME}_CXXFLAGS}" )

set ( _CONFIGURE_OPTIONS --prefix=<INSTALL_DIR>/$<CONFIG> --with-pic --disable-java --disable-python --disable-csharp --disable-examples )

set ( _CONFIGURE_OPTIONS ${_CONFIGURE_OPTIONS} $<$<CONFIG:Debug>:--enable-debug> )


# Not building in source directory, builds from <BINARY_DIR>/$<CONFIG>
if (${WIN32})
  string(REGEX REPLACE ";" " " _CONFIGURE_OPTIONS "${_CONFIGURE_OPTIONS}")
  # TODO find a better way to build antlr than loading bash to run the configure script.
  set ( CYGWIN_BASH c:\\cygwin64\\bin\\bash.exe )
  set ( _CONFIGURE_COMMAND ${CMAKE_COMMAND} -E make_directory $<CONFIG>
                   COMMAND ${CYGWIN_BASH} --login -c "cd <BINARY_DIR>/$<CONFIG> && <SOURCE_DIR>/configure ${_CONFIGURE_OPTIONS}" )
  set ( _BUILD_COMMAND ${CYGWIN_BASH} --login -c "cd <BINARY_DIR>/$<CONFIG> && make all verbose=1" )
  set ( _INSTALL_COMMAND ${CYGWIN_BASH} --login -c "cd <BINARY_DIR>/$<CONFIG> && make install" )
else()
   # TODO Configure trips up without MAKE
  set ( _CONFIGURE_COMMAND ${CMAKE_COMMAND} -E make_directory $<CONFIG>
               COMMAND ${CMAKE_COMMAND} -E chdir $<CONFIG> <SOURCE_DIR>/configure ${_CONFIGURE_OPTIONS} "CFLAGS=${${PROJECT_NAME}_CFLAGS}" "CXXFLAGS=${${PROJECT_NAME}_CXXFLAGS}" )
  set ( _BUILD_COMMAND ${CMAKE_COMMAND} -E chdir $<CONFIG> ${${PROJECT_NAME}_MAKE} all )
  set ( _INSTALL_COMMAND ${CMAKE_COMMAND} -E chdir $<CONFIG> ${${PROJECT_NAME}_MAKE} install )
endif()

include(ExternalProject)

ExternalProject_Add( ${PROJECT_NAME}
   URL ${${PROJECT_NAME}_URL}
   URL_HASH SHA256=${${PROJECT_NAME}_SHA265}
   UPDATE_COMMAND ""
   CONFIGURE_COMMAND "${_CONFIGURE_COMMAND}"
   BUILD_COMMAND "${_BUILD_COMMAND}"
   INSTALL_COMMAND "${_INSTALL_COMMAND}"
)

ExternalProject_Get_Property( ${PROJECT_NAME} SOURCE_DIR )
set( ${PROJECT_NAME}_SOURCE_DIR ${SOURCE_DIR} )
ExternalProject_Get_Property( ${PROJECT_NAME} INSTALL_DIR )
set( ${PROJECT_NAME}_INSTALL_DIR ${INSTALL_DIR}/$<CONFIG> )

set( DEPENDENCIES_${PROJECT_NAME}_DIR ${${PROJECT_NAME}_INSTALL_DIR} PARENT_SCOPE)
set( DEPENDENCIES_${PROJECT_NAME}_VERSION ${${PROJECT_NAME}_VERSION} PARENT_SCOPE)
set( ${PROJECT_NAME}_STATIC_LIB ${${PROJECT_NAME}_INSTALL_DIR}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}antlr${CMAKE_STATIC_LIBRARY_SUFFIX} PARENT_SCOPE)

ExternalProject_Add_Step( ${PROJECT_NAME} patches
    ALWAYS 0
    DEPENDEES download
    DEPENDERS patch
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/patches
    BYPRODUCTS ${${PROJECT_NAME}_SOURCE_DIR}/lib/cpp/antlr/CharScanner.hpp
    WORKING_DIRECTORY ${${PROJECT_NAME}_SOURCE_DIR}
    COMMAND ${PATCH} -u -p1 < ${CMAKE_CURRENT_SOURCE_DIR}/patches
)

