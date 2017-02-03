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
