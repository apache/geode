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
project(nativeclient.tests.fwklib)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(framework STATIC ${SOURCES})
set_target_properties(framework PROPERTIES FOLDER test)

target_link_libraries(framework
  PRIVATE
    ACE
  PUBLIC 
    apache-geode
    xerces-c
)

# Add PIC flags to link later with shared library
add_definitions(${CMAKE_CXX_COMPILE_OPTIONS_PIC})

#TODO very bad we include root of tests
include_directories(${CMAKE_SOURCE_DIR}/tests/cpp)
