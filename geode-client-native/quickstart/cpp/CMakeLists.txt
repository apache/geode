cmake_minimum_required(VERSION 3.4)
project(nativeclient.quickstart.cpp)

file(GLOB SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp")

#target_link_libraries(${QUICKSTART_UTILS_LIB}
#  PRIVATE
#    ${ACE_STATIC_LIB}
#  PUBLIC
#    gfcppcache
#)
#add_dependencies(${QUICKSTART_UTILS_LIB} ACE)

# Function to resolve both config and generate stage variables.
macro(generate_config INPUT TEMP OUTPUT)
    configure_file(${INPUT} ${TEMP})
    file(GENERATE OUTPUT ${OUTPUT}
      INPUT ${TEMP}
      CONDITION 1
    )
endmacro()

# TODO: Switch to the following when we support cmake install of nativeclient
#find_package(CPPCache REQUIRED)

set(SAV_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

foreach(FILE ${SOURCES})
  string(REGEX REPLACE "\\.cpp" "" QUICKSTART ${FILE})
  set(QUICKSTARTS ${QUICKSTARTS} ${QUICKSTART})
  
  set(EXTRA_SOURCES "")
  
  if (${QUICKSTART} STREQUAL "CqQuery") 
    add_definitions(-DBUILD_TESTOBJECT)
    set(EXTRA_SOURCES
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/Portfolio.cpp
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/Position.cpp)
    
  elseif (${QUICKSTART} STREQUAL "DataExpiration")
    add_definitions(-DBUILD_TESTOBJECT)
    set(EXTRA_SOURCES
      ${CMAKE_CURRENT_SOURCE_DIR}/plugins/SimpleCacheListener.cpp)   
      
  elseif (${QUICKSTART} STREQUAL "DurableClient")
    add_definitions(-DBUILD_TESTOBJECT)
    set(EXTRA_SOURCES
      ${CMAKE_CURRENT_SOURCE_DIR}/plugins/DurableCacheListener.cpp)  
      
  elseif (${QUICKSTART} STREQUAL "LoaderListenerWriter")
    add_definitions(-DBUILD_TESTOBJECT)
    set(EXTRA_SOURCES
      ${CMAKE_CURRENT_SOURCE_DIR}/plugins/SimpleCacheListener.cpp    
      ${CMAKE_CURRENT_SOURCE_DIR}/plugins/SimpleCacheLoader.cpp   
      ${CMAKE_CURRENT_SOURCE_DIR}/plugins/SimpleCacheWriter.cpp)   
     
  elseif (${QUICKSTART} STREQUAL "PdxAutoSerializer")
    add_definitions(-DBUILD_TESTOBJECT)
    set(EXTRA_SOURCES
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/testobject_PortfolioPdxAutoSerializable.cpp 
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/testobject_PositionPdxAutoSerializable.cpp       
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/PortfolioPdxAuto.cpp    
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/PositionPdxAuto.cpp)
    
  elseif (${QUICKSTART} STREQUAL "PdxRemoteQuery")
    add_definitions(-DBUILD_TESTOBJECT)
    set(EXTRA_SOURCES
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/PortfolioPdx.cpp    
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/PositionPdx.cpp)  
    
  elseif (${QUICKSTART} STREQUAL "PoolCqQuery")
    add_definitions(-DBUILD_TESTOBJECT)
    set(EXTRA_SOURCES
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/Portfolio.cpp    
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/Position.cpp)  
    
  elseif (${QUICKSTART} STREQUAL "PoolRemoteQuery")
    add_definitions(-DBUILD_TESTOBJECT)
    set(EXTRA_SOURCES
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/Portfolio.cpp    
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/Position.cpp) 
    
  elseif (${QUICKSTART} STREQUAL "RemoteQuery")
    add_definitions(-DBUILD_TESTOBJECT)
    set(EXTRA_SOURCES
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/Portfolio.cpp    
      ${CMAKE_CURRENT_SOURCE_DIR}/queryobjects/Position.cpp)       
    
  endif()
  
  add_executable(${QUICKSTART} ${FILE} ${EXTRA_SOURCES})
  
  set(CMAKE_CXX_FLAGS "${SAV_CMAKE_CXX_FLAGS} /I$(GFCPP)\\include")
  #include_directories("${GFCPP_INCLUDE}")
  


  target_link_libraries(${QUICKSTART}
    PRIVATE
    PUBLIC "$(GFCPP)\\lib\\gfcppcache.lib")  

  #set(TEST_DIR ${CMAKE_CURRENT_BINARY_DIR}/.tests/${TEST})
    
endforeach()

##TODO is this correct for config.h
#include_directories(${CMAKE_BINARY_DIR}/cppcache)
#include_directories(${CMAKE_BINARY_DIR}/cppcache/include)
##TODO this is really odd
#include_directories(${CMAKE_SOURCE_DIR}/cppcache)
#include_directories(${CMAKE_SOURCE_DIR}/cppcache/impl)
##TODO this is really bad that we include the root of tests
#include_directories(${CMAKE_SOURCE_DIR}/tests)
##TODO fix dependencies include paths
#include_directories(${DEPENDENCIES_ACE_DIR}/include)


