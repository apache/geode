cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests.cppcache)

set(TEST_UTILS_LIB test-cppcache-utils)
add_library(${TEST_UTILS_LIB} STATIC fw_dunit.cpp BBNamingContext.cpp CacheHelper.cpp)
target_link_libraries(${TEST_UTILS_LIB}
  PRIVATE
    ACE
  PUBLIC
    gfcppcache
    fwk
)
target_compile_definitions(${TEST_UTILS_LIB}
  PRIVATE
    $<MAKE_C_IDENTIFIER:${TEST_UTILS_LIB}>_static=1
)

add_library(unit_test_callbacks SHARED LibraryCallbacks.cpp)
target_link_libraries(unit_test_callbacks
  PRIVATE
    ACE
  PUBLIC
    gfcppcache
    c++11
)

enable_testing()

# Function to resolve both config and generate stage variables.
macro(generate_config INPUT TEMP OUTPUT)
    configure_file(${INPUT} ${TEMP})
    file(GENERATE OUTPUT ${OUTPUT}
      INPUT ${TEMP}
      CONDITION 1
    )
endmacro()

# Add lazy loaded shared library paths to test environment
foreach( lib ssl xerces-c sqlite )
  get_target_property(library_path ${lib} INTERFACE_LIBRARY_DIR)
  set(LD_LIBRARY_PATH ${LD_LIBRARY_PATH} ${library_path})
  get_target_property(runtime_path ${lib} INTERFACE_RUNTIME_DIR)
  set(PATH ${PATH} ${runtime_path})
endforeach()

set (PORT 20000)

file(GLOB SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "test*.cpp")
foreach(FILE ${SOURCES})
  string(REGEX REPLACE "\\.cpp" "" TEST ${FILE})
  set(TESTS ${TESTS} ${TEST})
  add_dependencies(integration-tests ${TEST})
  add_executable(${TEST} ${FILE})
  set_target_properties(${TEST} PROPERTIES FOLDER test/cppTests)  

  target_link_libraries(${TEST}
    PRIVATE
      ACE
      ${TEST_UTILS_LIB}
    PUBLIC
      gfcppcache
      testobject
      fwk
      c++11
  )
  
  # Some tests depend on these library
  add_dependencies(${TEST} securityImpl cryptoImpl DHImpl SqLiteImpl)
  
  set(TEST_DIR ${CMAKE_CURRENT_BINARY_DIR}/.tests/${TEST})
    
  math(EXPR PORT "${PORT} + 1")
    
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
        
  set_property(TEST ${TEST} PROPERTY LABELS STABLE)
endforeach()

configure_file(CTestCustom.cmake.in CTestCustom.cmake)

#TODO this is really bad that we include the root of tests
include_directories(${CMAKE_SOURCE_DIR}/tests/cpp)

set_property(TEST testDataOutput PROPERTY LABELS STABLE QUICK)
set_property(TEST testFWHelper PROPERTY LABELS STABLE QUICK)
set_property(TEST testLRUList PROPERTY LABELS STABLE QUICK)
set_property(TEST testSystemProperties PROPERTY LABELS STABLE QUICK)
set_property(TEST testLogger PROPERTY LABELS STABLE QUICK)
set_property(TEST testEntriesMap PROPERTY LABELS STABLE QUICK)
set_property(TEST testCache PROPERTY LABELS STABLE QUICK)
set_property(TEST testLinkage PROPERTY LABELS STABLE QUICK)
set_property(TEST testRegionTemplateArgs PROPERTY LABELS STABLE QUICK)
set_property(TEST testRegionMap PROPERTY LABELS STABLE QUICK)
set_property(TEST testXmlCacheCreationWithRefid PROPERTY LABELS STABLE QUICK)
set_property(TEST testAttributesFactory PROPERTY LABELS STABLE QUICK)
set_property(TEST testXmlCacheCreationWithOverFlow PROPERTY LABELS STABLE QUICK)
set_property(TEST testNativeCompareBasic PROPERTY LABELS STABLE QUICK)
set_property(TEST testConnect PROPERTY LABELS STABLE QUICK)
set_property(TEST testSharedPtr PROPERTY LABELS STABLE QUICK)
set_property(TEST testThinClientRemoveAllLocal PROPERTY LABELS STABLE QUICK)
set_property(TEST testDunit PROPERTY LABELS STABLE QUICK)
set_property(TEST testAttributesMutator PROPERTY LABELS STABLE QUICK)
set_property(TEST testEntriesMapForVersioning PROPERTY LABELS STABLE QUICK)
set_property(TEST testSpinLock PROPERTY LABELS STABLE QUICK)

set_property(TEST testOverflowPutGetSqLite PROPERTY LABELS FLAKY)
set_property(TEST testThinClientAfterRegionLive PROPERTY LABELS FLAKY)
set_property(TEST testThinClientCacheables PROPERTY LABELS FLAKY)
set_property(TEST testThinClientCq PROPERTY LABELS FLAKY)
set_property(TEST testThinClientCqFailover PROPERTY LABELS FLAKY)
set_property(TEST testThinClientCqHAFailover PROPERTY LABELS FLAKY)
set_property(TEST testThinClientDistOpsUpdateLocatorList PROPERTY LABELS FLAKY)
set_property(TEST testThinClientDurableConnect PROPERTY LABELS FLAKY)
set_property(TEST testThinClientDurableDisconnectNormal PROPERTY LABELS FLAKY)
set_property(TEST testThinClientDurableDisconnectTimeout PROPERTY LABELS FLAKY)
set_property(TEST testThinClientDurableFailoverClientClosedNoRedundancy PROPERTY LABELS FLAKY)
set_property(TEST testThinClientDurableFailoverClientNotClosedRedundancy PROPERTY LABELS FLAKY)
set_property(TEST testThinClientDurableKeepAliveFalseTimeout PROPERTY LABELS FLAKY)
set_property(TEST testThinClientDurableKeepAliveTrueNormal PROPERTY LABELS FLAKY)
set_property(TEST testThinClientDurableReconnect PROPERTY LABELS FLAKY)
set_property(TEST testThinClientFailover PROPERTY LABELS FLAKY)
set_property(TEST testThinClientFailover2 PROPERTY LABELS FLAKY)
set_property(TEST testThinClientFailoverInterest PROPERTY LABELS FLAKY)
set_property(TEST testThinClientFailoverInterest2 PROPERTY LABELS FLAKY)
set_property(TEST testThinClientFailoverRegex PROPERTY LABELS FLAKY)
set_property(TEST testThinClientFixedPartitionResolver PROPERTY LABELS FLAKY)
set_property(TEST testThinClientHADistOps PROPERTY LABELS FLAKY)
set_property(TEST testThinClientHAEventIDMap PROPERTY LABELS FLAKY)
set_property(TEST testThinClientHAFailover PROPERTY LABELS FLAKY)
set_property(TEST testThinClientHAMixedRedundancy PROPERTY LABELS FLAKY)
set_property(TEST testThinClientHAQueryFailover PROPERTY LABELS FLAKY)
set_property(TEST testThinClientLRUExpiration PROPERTY LABELS FLAKY)
set_property(TEST testThinClientLargePutAllWithCallBackArg PROPERTY LABELS FLAKY)
set_property(TEST testThinClientLocatorFailover PROPERTY LABELS FLAKY)
set_property(TEST testThinClientMultiDS PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPRPutAllFailover PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPartitionResolver PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPdxDeltaWithNotification PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPdxInstance PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPdxTests PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPdxTestsWithAuto PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPoolExecuteHAFunction PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPoolExecuteHAFunctionPrSHOP PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPoolRedundancy PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPoolServer PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPutAll PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPutAllPRSingleHop PROPERTY LABELS FLAKY)
set_property(TEST testThinClientPutAllWithCallBackArgWithoutConcurrency PROPERTY LABELS FLAKY)
set_property(TEST testThinClientSecurityCQAuthorizationMU PROPERTY LABELS FLAKY)
set_property(TEST testThinClientSecurityMultiUserTest PROPERTY LABELS FLAKY)
set_property(TEST testThinClientTXFailover PROPERTY LABELS FLAKY)
set_property(TEST testThinClientTransactionsWithSticky PROPERTY LABELS FLAKY)
set_property(TEST testThinClientTransactionsXA PROPERTY LABELS FLAKY)
set_property(TEST testTimedSemaphore PROPERTY LABELS FLAKY)

set_property(TEST testFwPerf PROPERTY LABELS OMITTED)
set_property(TEST testThinClientCqDurable PROPERTY LABELS OMITTED)
set_property(TEST testThinClientExecuteFunctionPrSHOP PROPERTY LABELS OMITTED)
set_property(TEST testThinClientGatewayTest PROPERTY LABELS OMITTED)
set_property(TEST testThinClientHAFailoverRegex PROPERTY LABELS OMITTED)
set_property(TEST testThinClientPRSingleHop PROPERTY LABELS OMITTED)
set_property(TEST testThinClientPoolAttrTest PROPERTY LABELS OMITTED)
set_property(TEST testThinClientPoolExecuteFunction PROPERTY LABELS OMITTED)
set_property(TEST testThinClientPoolExecuteFunctionPrSHOP PROPERTY LABELS OMITTED)
set_property(TEST testThinClientPoolLocator PROPERTY LABELS OMITTED)
set_property(TEST testThinClientPutWithDelta PROPERTY LABELS OMITTED)
set_property(TEST testThinClientRemoteQueryTimeout PROPERTY LABELS OMITTED)
set_property(TEST testThinClientRemoveOps PROPERTY LABELS OMITTED)
set_property(TEST testThinClientSSLWithSecurityAuthz PROPERTY LABELS OMITTED)
set_property(TEST testThinClientSecurityAuthentication PROPERTY LABELS OMITTED)
set_property(TEST testThinClientSecurityAuthenticationMU PROPERTY LABELS OMITTED)
set_property(TEST testThinClientSecurityAuthorization PROPERTY LABELS OMITTED)
set_property(TEST testThinClientSecurityAuthorizationMU PROPERTY LABELS OMITTED)
set_property(TEST testThinClientSecurityDH PROPERTY LABELS OMITTED)
set_property(TEST testThinClientSecurityDH_MU PROPERTY LABELS OMITTED)
set_property(TEST testThinClientSecurityDurableCQAuthorizationMU PROPERTY LABELS OMITTED)
set_property(TEST testThinClientSecurityPostAuthorization PROPERTY LABELS OMITTED)
set_property(TEST testThinClientTicket303 PROPERTY LABELS OMITTED)
set_property(TEST testThinClientTicket304 PROPERTY LABELS OMITTED)
set_property(TEST testThinClientTracking PROPERTY LABELS OMITTED)
set_property(TEST testThinClientWriterException PROPERTY LABELS OMITTED)

add_custom_target(run-stable-cppcache-integration-tests
  COMMAND ctest -C $<CONFIGURATION> -L STABLE
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  USES_TERMINAL)
add_dependencies(run-integration-tests run-stable-cppcache-integration-tests)
set_target_properties(run-stable-cppcache-integration-tests PROPERTIES
    EXCLUDE_FROM_ALL TRUE
    EXCLUDE_FROM_DEFAULT_BUILD TRUE
)

add_custom_target(run-quick-cppcache-integration-tests
  COMMAND ctest -C $<CONFIGURATION> -L QUICK -j 4
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  USES_TERMINAL)
set_target_properties(run-quick-cppcache-integration-tests PROPERTIES
    EXCLUDE_FROM_ALL TRUE
    EXCLUDE_FROM_DEFAULT_BUILD TRUE
)
