cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests.cppcache)

#TODO move to config.h when not building shared ace
add_definitions(-D__ACE_INLINE__)
add_definitions(-DACE_AS_STATIC_LIBS)

# Common sources for all tests.
set(TEST_UTILS_LIB test-cppcache-utils)
add_library(${TEST_UTILS_LIB} STATIC fw_dunit.cpp BBNamingContext.cpp CacheHelper.cpp)
target_link_libraries(${TEST_UTILS_LIB}
  PRIVATE
    ${ACE_STATIC_LIB}
  PUBLIC
    gfcppcache
    fwk
)
add_dependencies(${TEST_UTILS_LIB} ACE)

add_library(unit_test_callbacks SHARED LibraryCallbacks.cpp)
target_link_libraries(unit_test_callbacks
  PRIVATE
    ${ACE_STATIC_LIB}
  PUBLIC
    gfcppcache
)
add_dependencies(unit_test_callbacks ACE)


enable_testing()

# Function to resolve both config and generate stage variables.
macro(generate_config INPUT TEMP OUTPUT)
    configure_file(${INPUT} ${TEMP})
    file(GENERATE OUTPUT ${OUTPUT}
      INPUT ${TEMP}
      CONDITION 1
    )
endmacro()

set (PORT 20000)

file(GLOB SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "test*.cpp")
foreach(FILE ${SOURCES})
  string(REGEX REPLACE "\\.cpp" "" TEST ${FILE})
  set(TESTS ${TESTS} ${TEST})
    
  add_executable(${TEST} ${FILE})
  set_target_properties(${TESTS} PROPERTIES FOLDER test/cppTests)  

  target_link_libraries(${TEST}
    PRIVATE
#TODO ACE only needed for 1 test
      ${ACE_STATIC_LIB}
    PUBLIC
      ${TEST_UTILS_LIB}
      gfcppcache
      testobject
      fwk
  )
  
  # Some tests depend on these library
  add_dependencies(${TEST} securityImpl cryptoImpl DHImpl SqLiteImpl)
  

  set(TEST_DIR ${CMAKE_CURRENT_BINARY_DIR}/.tests/${TEST})
    
  math(EXPR PORT "${PORT} + 1")
    
  # TODO path to DYLD ACE
  # TODO path to Gemfire
  # TODO build javaobject.jar

  if (WIN32)
    set (TEST_SCRIPT_SUFFIX .bat)
    generate_config(${CMAKE_CURRENT_SOURCE_DIR}/test.bat.in ${CMAKE_CURRENT_BINARY_DIR}/.${TEST}.bat.in $<TARGET_FILE:${TEST}>${TEST_SCRIPT_SUFFIX})

    set(TEST_COMMAND $<TARGET_FILE:${TEST}>${TEST_SCRIPT_SUFFIX})

  else()
    set (TEST_SCRIPT_SUFFIX .sh)
    generate_config(${CMAKE_CURRENT_SOURCE_DIR}/test.sh.in ${CMAKE_CURRENT_BINARY_DIR}/.${TEST}.sh.in $<TARGET_FILE:${TEST}>${TEST_SCRIPT_SUFFIX})

    # TODO fix file permissions on .sh
    set(TEST_COMMAND bash $<TARGET_FILE:${TEST}>${TEST_SCRIPT_SUFFIX})
    
  endif()

  add_test(NAME ${TEST}
      COMMAND ${TEST_COMMAND}
  )
        
endforeach()

configure_file(CTestCustom.cmake.in CTestCustom.cmake)

#TODO is this correct for config.h
include_directories(${CMAKE_BINARY_DIR}/cppcache)
include_directories(${CMAKE_BINARY_DIR}/cppcache/include)
#TODO this is really odd
include_directories(${CMAKE_SOURCE_DIR}/cppcache)
include_directories(${CMAKE_SOURCE_DIR}/cppcache/impl)
#TODO this is really bad that we include the root of tests
include_directories(${CMAKE_SOURCE_DIR}/tests)
#TODO fix dependencies include paths
include_directories(${DEPENDENCIES_ACE_DIR}/include)


