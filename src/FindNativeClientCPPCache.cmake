#.rst:
# FindNativeClientCPPCache
# ------------------------
#
# Try to find the NativeClient cppcache library
#
# Once done this will define
#
# ::
#
#   NATIVECLIENT_CPPCACHE_FOUND - true when cmake has found the NativeClient CPPCache library
#   NATIVECLIENT_CPPCACHE_INCLUDE_DIR - The NativeClient include directory
#   NATIVECLIENT_CPPCACHE_LIBRARIES - The libraries needed to use NativeClient CPPCache library
#   NATIVECLIENT_CPPCACH_DEFINITIONS - Compiler switches required for using NativeClient CPPCache library
#   NATIVECLIENT_CPPCACH_VERSION_STRING - the version of NativeClient CPPCache library found

#=============================================================================
# Copyright 2006-2009 Kitware, Inc.
# Copyright 2006 Alexander Neundorf <neundorf@kde.org>
#
# Distributed under the OSI-approved BSD License (the "License");
# see accompanying file Copyright.txt for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the License for more information.
#=============================================================================
# (TODO: To distribute this file outside of CMake, substitute the full
#  License text for the above reference.)

find_path(LIBXML2_INCLUDE_DIR NAMES libxml/xpath.h
   HINTS
   ${PC_LIBXML_INCLUDEDIR}
   ${PC_LIBXML_INCLUDE_DIRS}
   PATH_SUFFIXES libxml2
   )

find_library(LIBXML2_LIBRARIES NAMES xml2 libxml2
   HINTS
   ${PC_LIBXML_LIBDIR}
   ${PC_LIBXML_LIBRARY_DIRS}
   )

find_program(LIBXML2_XMLLINT_EXECUTABLE xmllint)

set( NATIVECLIENT_CPPCACHE_VERSION_STRING "9.0" )
set( NATIVECLIENT_CPPCACHE_FOUND "YES" )
set( NATIVECLIENT_CPPCACHE_INCLUDE_DIR  ${CMAKE_CURRENT_BINARY_DIRECTORY}/include ) # TODO: Replace with install directory
set( NATIVECLIENT_CPPCACHE_LIBRARIES  ${GTK_gtk_LIBRARY}
                    ${GTK_gdk_LIBRARY}
                    ${GTK_glib_LIBRARY} )
elseif(LIBXML2_INCLUDE_DIR AND EXISTS "${LIBXML2_INCLUDE_DIR}/libxml/xmlversion.h")
    file(STRINGS "${LIBXML2_INCLUDE_DIR}/libxml/xmlversion.h" libxml2_version_str
         REGEX "^#define[\t ]+LIBXML_DOTTED_VERSION[\t ]+\".*\"")

    string(REGEX REPLACE "^#define[\t ]+LIBXML_DOTTED_VERSION[\t ]+\"([^\"]*)\".*" "\\1"
           LIBXML2_VERSION_STRING "${libxml2_version_str}")
    unset(libxml2_version_str)
endif()

# handle the QUIETLY and REQUIRED arguments and set LIBXML2_FOUND to TRUE if
# all listed variables are TRUE
include(${CMAKE_CURRENT_LIST_DIR}/FindPackageHandleStandardArgs.cmake)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LibXml2
                                  REQUIRED_VARS LIBXML2_LIBRARIES LIBXML2_INCLUDE_DIR
                                  VERSION_VAR LIBXML2_VERSION_STRING)

mark_as_advanced(LIBXML2_INCLUDE_DIR LIBXML2_LIBRARIES LIBXML2_XMLLINT_EXECUTABLE)
