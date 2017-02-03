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
project(nativeclient.quickstart.csharp)

find_package(NativeClient REQUIRED PATHS ..)

# Cache the path to the NativeClient product
set (GFCPP CACHE ${NATIVECLIENT_DIR} PATH REQUIRED)

# Set the .NET Target Framework (Note: This should match the build for Apache.Geode.)
set (DOTNET_TARGET_FRAMEWORK_VERSION "v4.5.2")

file(GLOB_RECURSE CSPROJECTS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.csproj.in")

foreach(FILE ${CSPROJECTS})
  string(REGEX REPLACE "\\.csproj.in" "" PROJDIR ${FILE})
  string(REGEX REPLACE ".*/" "" PROJNAME ${PROJDIR})
  get_filename_component(Dir ${FILE} DIRECTORY)  
  get_filename_component(File ${FILE} NAME_WE)
  set(OutFile "${Dir}/${File}.csproj")
  
  file(COPY vsprojects/${PROJNAME}/Properties DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/vsprojects/${PROJNAME})
  
  configure_file(${FILE} ${OutFile})
  include_external_msproject(
      ${PROJNAME}  ${OutFile}
      TYPE FAE04EC0-301F-11D3-BF4B-00C04F79EFBC)
  set_target_properties(${PROJNAME} PROPERTIES FOLDER csharp)     
  
  configure_file("app.config.in"  ${PROJNAME}.exe.config)

endforeach()

# Configure the run script
configure_file(${CMAKE_SOURCE_DIR}/../runcs.bat.in "${CMAKE_BINARY_DIR}/runcs.bat")

file(COPY ../XMLs DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
