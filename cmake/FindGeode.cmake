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
##
# FindGeode CMake find module.
##

set(_GEODE_ROOT "")
if(GEODE_ROOT AND IS_DIRECTORY "${GEODE_ROOT}")
  set(_GEODE_ROOT "${GEODE_ROOT}")
  set(_GEODE_ROOT_EXPLICIT 1)
else()
  set(_ENV_GEODE_ROOT "")
  if(DEFINED ENV{GEODE_ROOT})
    file(TO_CMAKE_PATH "$ENV{GEODE_ROOT}" _ENV_GEODE_ROOT)
  elseif(DEFINED ENV{GEODE_HOME})
    file(TO_CMAKE_PATH "$ENV{GEODE_HOME}" _ENV_GEODE_ROOT)
  elseif(DEFINED ENV{GEODE})
    file(TO_CMAKE_PATH "$ENV{GEODE}" _ENV_GEODE_ROOT)
  endif()
  if(_ENV_GEODE_ROOT AND IS_DIRECTORY "${_ENV_GEODE_ROOT}")
    set(_GEODE_ROOT "${_ENV_GEODE_ROOT}")
    set(_GEODE_ROOT_EXPLICIT 0)
  endif()
  unset(_ENV_GEODE_ROOT)
endif()

set(_GEODE_HINTS)
if(_GEODE_ROOT)
  set(_GEODE_HINTS ${_GEODE_ROOT}/bin)
endif()

set(_GEODE_PATHS
  /geode/bin
  /apache-geode/bin
  /usr/geode/bin
  /usr/apache-geode/bin
  /usr/local/geode/bin
  /usr/local/apache-geode/bin
  /opt/geode/bin
  /opt/apache-geode/bin
  /opt/local/geode/bin
  /opt/local/apache-geode/bin
)

if(WIN32)
  set(_GEODE_NAMES gfsh.bat)
else()
  set(_GEODE_NAMES gfsh)
endif()

find_program(Geode_gfsh_EXECUTABLE
  NAMES ${_GEODE_NAMES}
  HINTS ${_GEODE_HINTS}
  PATHS ${_GEODE_PATHS}
)

if(Geode_gfsh_EXECUTABLE)
  execute_process(COMMAND ${Geode_gfsh_EXECUTABLE} version
    RESULT_VARIABLE res
    OUTPUT_VARIABLE var
    ERROR_VARIABLE var
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_STRIP_TRAILING_WHITESPACE)
  if(res)
    # TODO error checking
  else()
    if(var MATCHES "([0-9]+\\.[0-9]+\\.[0-9]+)")
      set(Geode_VERSION "${CMAKE_MATCH_1}")
    else()
      # TODO version parsing error
    endif()
    
    get_filename_component(Geode_PATH ${Geode_gfsh_EXECUTABLE} REALPATH)
    get_filename_component(Geode_PATH ${Geode_PATH} DIRECTORY)
    get_filename_component(Geode_PATH ${Geode_PATH}/.. REALPATH)
    
    set(Geode_CLASSPATH)
    if(EXISTS ${Geode_PATH}/lib/geode-dependencies.jar)
      set(Geode_CLASSPATH ${Geode_PATH}/lib/geode-dependencies.jar)
    endif()
  endif()
endif()

mark_as_advanced(
  Geode_gfsh_EXECUTABLE
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args (Geode
  FOUND_VAR Geode_FOUND
  REQUIRED_VARS Geode_gfsh_EXECUTABLE Geode_PATH Geode_CLASSPATH
  VERSION_VAR Geode_VERSION
)
