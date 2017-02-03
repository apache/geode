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

#find_path(LIBXML2_INCLUDE_DIR NAMES libxml/xpath.h
#   HINTS
#   ${PC_LIBXML_INCLUDEDIR}
#   ${PC_LIBXML_INCLUDE_DIRS}
#   PATH_SUFFIXES libxml2
#   )
#
#find_library(LIBXML2_LIBRARIES NAMES xml2 libxml2
#   HINTS
#   ${PC_LIBXML_LIBDIR}
#   ${PC_LIBXML_LIBRARY_DIRS}
#   )

set( NATIVECLIENT_CPPCACHE_VERSION_STRING "9.0" )
set( NATIVECLIENT_CPPCACHE_FOUND "YES" )
set( NATIVECLIENT_CPPCACHE_INCLUDE_DIR ../../../include )
set( NATIVECLIENT_CPPCACHE_LIBRARIES ../../../lib )
set( NATIVECLIENT_CPPCACHE_BINARIES_DIR ../../../bin )

