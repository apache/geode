cmake_minimum_required( VERSION 3.3 )
project(gfcppcache_unittests)

file(GLOB_RECURSE SOURCES "*.cpp")

set(UNITTEST gfcppcache_unittests)
add_executable(${UNITTEST} ${SOURCES})
add_dependencies(unit-tests ${UNITTEST})

target_link_libraries(${UNITTEST}
  gfcppcache-static
  gtest
  gtest_main
  c++11
)

enable_testing()

# Function to resolve both config and generate stage variables.
macro(generate_config INPUT TEMP OUTPUT)
  configure_file(${INPUT} ${TEMP})
  file(GENERATE OUTPUT ${OUTPUT} INPUT ${TEMP} CONDITION 1)
endmacro()

if (WIN32)
  set (UNITTEST_SCRIPT_SUFFIX .bat)
  generate_config(${CMAKE_CURRENT_SOURCE_DIR}/${UNITTEST}.bat.in ${CMAKE_CURRENT_BINARY_DIR}/.${UNITTEST}.bat.in $<TARGET_FILE:${UNITTEST}>${UNITTEST_SCRIPT_SUFFIX})

  set(UNITTEST_COMMAND $<TARGET_FILE:${UNITTEST}>${UNITTEST_SCRIPT_SUFFIX})
else()
  set (UNITTEST_SCRIPT_SUFFIX .sh)
  generate_config(${CMAKE_CURRENT_SOURCE_DIR}/${UNITTEST}.sh.in ${CMAKE_CURRENT_BINARY_DIR}/.${UNITTEST}.sh.in $<TARGET_FILE:${UNITTEST}>${UNITTEST_SCRIPT_SUFFIX})

  # TODO fix file permissions on .sh
  set(UNITTEST_COMMAND bash $<TARGET_FILE:${UNITTEST}>${UNITTEST_SCRIPT_SUFFIX})
endif()

add_test(NAME ${UNITTEST} COMMAND ${UNITTEST_COMMAND})

if (MSVC)
  target_compile_options(${UNITTEST} PRIVATE "/MD$<$<CONFIG:Debug>:d>")
endif()

add_custom_target(run-cppcache-unit-tests
  COMMAND ctest -C $<CONFIGURATION> ${CTEST_UNITTEST_VERBOSITY}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  USES_TERMINAL)
add_dependencies(run-unit-tests run-cppcache-unit-tests)
set_target_properties(run-cppcache-unit-tests PROPERTIES
    EXCLUDE_FROM_ALL TRUE
    EXCLUDE_FROM_DEFAULT_BUILD TRUE
)
