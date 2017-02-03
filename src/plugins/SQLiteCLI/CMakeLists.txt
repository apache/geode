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
project(nativeclient.plugins.SQLiteCLI)

# Set the .NET Target Framework (Note: This should match the build for Apache.Geode.)
set (DOTNET_TARGET_FRAMEWORK_VERSION "v4.5.2")

foreach(var CMAKE_CURRENT_SOURCE_DIR CMAKE_SOURCE_DIR CMAKE_BINARY_DIR CMAKE_CURRENT_BINARY_DIR)
  file(TO_NATIVE_PATH ${${var}} ${var}_NATIVE)
endforeach()

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/SQLiteCLI.csproj.in ${CMAKE_CURRENT_BINARY_DIR}/SQLiteCLI/SQLiteCLI.csproj)  
include_external_msproject(
      SQLiteCLI  ${CMAKE_CURRENT_BINARY_DIR}/SQLiteCLI/SQLiteCLI.csproj
      TYPE FAE04EC0-301F-11D3-BF4B-00C04F79EFBC)
