cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests.clicache)

set (NUNIT "c:/tools/thirdparty/windows64/NUnit-2.6.0")

file(GLOB_RECURSE CSPROJECTS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.csproj.in")

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

if (${BUILD_CLI})
  add_subdirectory(NativeWrapper)
  add_subdirectory(PkcsWrapper)
  add_subdirectory(QueryHelper)
endif()

