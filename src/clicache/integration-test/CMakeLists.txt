cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests.clicache)

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
  configure_file(${FILE} ${CMAKE_CURRENT_BINARY_DIR}/${PROJNAME}.csproj)
  include_external_msproject(
      ${PROJNAME}  ${CMAKE_CURRENT_BINARY_DIR}/${PROJNAME}.csproj
      TYPE FAE04EC0-301F-11D3-BF4B-00C04F79EFBC)
  set_target_properties(${PROJNAME} PROPERTIES FOLDER test/cliTests)

endforeach()

enable_testing()

set (TEST_SCRIPT_SUFFIX .bat)
file(GLOB SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cs")

foreach(FILE ${SOURCES})
  # Find all non-deprecated tests
  file(STRINGS ${FILE} HasTests REGEX "\\[Test\\]")

  LIST(LENGTH HasTests length)
  if (${length} GREATER 0)
      file(STRINGS ${FILE} IsDeprecated REGEX "\\[Category.*deprecated.*\\]")
	  if (NOT IsDeprecated)

		  # Get the namespace
		  file(STRINGS ${FILE} NameSpaceLine REGEX "namespace GemStone.GemFire.Cache.UnitTests")
		  string(REGEX REPLACE "namespace.*GemStone" "GemStone" NAMESPACE ${NameSpaceLine})

          string(REGEX REPLACE "\\.cs" "" TEST ${FILE})        
          set(TESTS ${TESTS} ${TEST})
          set(TEST_DIR ${CMAKE_CURRENT_BINARY_DIR}/.tests/${TEST})

		  # Get the test class (NewAPI tests have N in the file name but not test class name)
		  string(REGEX REPLACE "N$" "" TESTCLASS ${TEST})

          configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test.bat.in ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_BUILD_TYPE}/${TEST}${TEST_SCRIPT_SUFFIX})
          set(TEST_COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_BUILD_TYPE}/${TEST}${TEST_SCRIPT_SUFFIX})
          add_test(NAME ${TEST}
              COMMAND ${TEST_COMMAND}
          )
          set_property(TEST ${TEST} PROPERTY LABELS STABLE)
	  endif()
  endif()
endforeach()

# Label any flaky tests here
set_property(TEST ThinClientCqTestsN PROPERTY LABELS FLAKY)

# Label any tests that always fail here
set_property(TEST OverflowTestsN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientDeltaTestN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientDocExamplesN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientDurableTestsN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientFunctionExecutionTestsN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientHARegionTestsN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientPdxTests PROPERTY LABELS OMITTED)
set_property(TEST ThinClientRegionTestsN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientSecurityAuthTestsMUN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientSecurityAuthTestsN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientSecurityAuthTestsMUN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientSecurityAuthzTestsMUN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientSecurityAuthzTestsN PROPERTY LABELS OMITTED)
set_property(TEST ThinClientStatisticTestsN PROPERTY LABELS OMITTED)

add_custom_target(run-stable-clicache-integration-tests
  COMMAND ctest -C $<CONFIGURATION> -L STABLE
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  USES_TERMINAL)
add_dependencies(run-integration-tests run-stable-clicache-integration-tests)
set_target_properties(run-stable-clicache-integration-tests PROPERTIES EXCLUDE_FROM_ALL TRUE EXCLUDE_FROM_DEFAULT_BUILD TRUE)

