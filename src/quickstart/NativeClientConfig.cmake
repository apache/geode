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
if (DEFINED ENV{GFCPP})
    MESSAGE(STATUS "Using GFCPP=$ENV{GFCPP} as product directory.")
    set( NCProductDir $ENV{GFCPP} )
else()
    if (WIN32)
        file( GLOB NCCandidates /apache-geode-nativeclient* )
        #peek registry https://cmake.org/Wiki/CMake_FAQ#How%5Fcan%5FI%5Fget%5Fa%5Fwindows%5Fregistry%5Fkey%5F.3F
    else()
        file( GLOB NCCandidates /apache-geode-nativeclient* )
        if (NOT NCCandidates)
            file( GLOB NCCandidates /geode-nativeclient* )
            if (NOT NCCandidates)
                file( GLOB NCCandidates /usr/local/geode-nativeclient* )
                if (NOT NCCandidates)
                    file( GLOB NCCandidates /Applications/Apache_Geode_NativeClient* )
                endif()
            endif()
        endif()
    endif()

    if (NCCandidates)
        list(SORT NCCandidates)
        list(REVERSE NCCandidates)
        list(GET NCCandidates 0 NCProductDir)
        MESSAGE(STATUS "Derived ${NCProductDir} as product directory.")
    else()
        MESSAGE(FATAL_ERROR "GFCPP is not set and could not be derived!")
    endif()
endif()

# run gfcpp.exe and make sure we get a version back

#set( NATIVECLIENT_VERSION_STRING "9.0" )
#set( NATIVECLIENT_FOUND "YES" )
set( NATIVECLIENT_DIR ${NCProductDir} )
set( NATIVECLIENT_INCLUDE_DIR ${NCProductDir}/include )
set( NATIVECLIENT_LIBRARIES ${NCProductDir}/lib )
set( NATIVECLIENT_BINARIES_DIR ${NCProductDir}/bin )
