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
cmake_minimum_required(VERSION 3.4)
project(QueryWrapper)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(QueryWrapper SHARED ${SOURCES})

#TODO get external project library names
target_link_libraries(QueryWrapper
  PUBLIC 
    Apache.Geode
    c++11
)

include_directories(${CMAKE_SOURCE_DIR}/clicache/src)
include_directories(${CMAKE_SOURCE_DIR}/tests/clicache)
include_directories(${CMAKE_SOURCE_DIR}/cppcache/integration-test)

set_target_properties(QueryWrapper PROPERTIES FOLDER test/cliTests)

# TODO discover framework
set( DOT_NET_FRAMEWORK "C:/Program Files (x86)/Reference Assemblies/Microsoft/Framework/.NETFramework/v4.0")

# TODO macro add_dotnet_assemble(taget assemblyname)
set( ASM_PATH ${ASM_PATH} "/FU ${DOT_NET_FRAMEWORK}/System.dll")
set( ASM_PATH ${ASM_PATH} "/FU ${DOT_NET_FRAMEWORK}/System.Xml.dll")
target_compile_options(QueryWrapper PRIVATE ${ASM_PATH})

#TODO make CLR macro to do all the CLR flags below, add_clr(target)
string(REPLACE "/EHsc" "/EHa" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
#TODO evaluate the correct CMAKE_CXX_FLAGS_<CONFIG> settings for .NET
string(REPLACE "/RTC1" "" CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /clr /wd4947")

#TODO make macro for strong name key, add_strong_nake_key(traget keyfile)
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${SHARED_LINKER_FLAGS_STRONG_KEY}")
