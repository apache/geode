cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests)

add_subdirectory(NativeWrapper)
add_subdirectory(PkcsWrapper)
add_subdirectory(QueryHelper)

set (NUNIT "C:\\Program Files (x86)\\NUnit 2.6.4")

if (64 EQUAL ${BUILD_BITS})
  set (NUNIT_CONSOLE "nunit-console")
else()
  set (NUNIT_CONSOLE "nunit-console-x86")
endif()

# Set the .NET Target Framework (Note: This should match the build for gfclicache.)
set (DOTNET_TARGET_FRAMEWORK_VERSION "v4.5.2")

foreach(var CMAKE_CURRENT_SOURCE_DIR CMAKE_SOURCE_DIR CMAKE_BINARY_DIR CMAKE_CURRENT_BINARY_DIR)
  file(TO_NATIVE_PATH ${${var}} ${var}_NATIVE)
endforeach()

file(GLOB_RECURSE CSPROJECTS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.csproj.in")
if(EXISTS "${STRONG_NAME_KEY}")
  set(STRONG_NAME_KEY_ENABLED true)
endif()

foreach(FILE ${CSPROJECTS})
  # TODO: make this a one line MATCH statement
  string(REGEX REPLACE "\\.csproj.in" "" PROJDIR ${FILE})
  string(REGEX REPLACE ".*/" "" PROJNAME ${PROJDIR})
  configure_file(${FILE} ${CMAKE_CURRENT_BINARY_DIR}/${PROJNAME}/${PROJNAME}.csproj)
  include_external_msproject(
      ${PROJNAME}  ${CMAKE_CURRENT_BINARY_DIR}/${PROJNAME}/${PROJNAME}.csproj
      TYPE FAE04EC0-301F-11D3-BF4B-00C04F79EFBC)
  set_target_properties(${PROJNAME} PROPERTIES FOLDER test/cliTests)

endforeach()