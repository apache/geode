cmake_minimum_required(VERSION 3.4)
project(nativeclient.quickstart.cpp)

find_package(NativeClient REQUIRED PATHS ..)

# Cache the path to the NativeClient product
set (GFCPP CACHE ${NATIVECLIENT_DIR} PATH REQUIRED)

if (APPLE)
    set(DYNAMIC_LIBRARY_PATH DYLD_LIBRARY_PATH=${NATIVECLIENT_DIR}/lib)
elseif(UNIX)
    set(DYNAMIC_LIBRARY_PATH LD_LIBRARY_PATH=${NATIVECLIENT_DIR}/lib)
endif()

macro(add_pdxautoserializer NAMESPACE CLASS HEADER SUFFIX CLASSNAMESTR)
    set(_OUTPUT ${CMAKE_SOURCE_DIR}/queryobjects/${NAMESPACE}_${CLASS}${SUFFIX}.cpp)
    set(_INPUT ${HEADER})

    add_custom_command(OUTPUT ${_OUTPUT}
        COMMAND ${DYNAMIC_LIBRARY_PATH} ${NATIVECLIENT_BINARIES_DIR}/pdxautoserializer --outDir=${CMAKE_SOURCE_DIR}/queryobjects ${CMAKE_SOURCE_DIR}/queryobjects/${HEADER}
        DEPENDS ${CMAKE_SOURCE_DIR}/queryobjects/${_INPUT}
    )
endmacro()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(CheckCXXCompilerFlag)
include(CheckCCompilerFlag)

set(CMAKE_REQUIRED_LIBRARIES -m64)
check_c_compiler_flag(-m64 CFLAGS_M64_ALLOWED)
check_cxx_compiler_flag(-m64 CXXFLAGS_M64_ALLOWED)
set(CMAKE_REQUIRED_LIBRARIES)

check_c_compiler_flag(-mt CFLAGS_mt_ALLOWED)

if (CFLAGS_M64_ALLOWED AND CXXFLAGS_M64_ALLOWED)
  add_compile_options(-m64)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -m64")
endif()

if (CFLAGS_mt_ALLOWED)
  add_compile_options(-mt)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "SunPro")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -std=c++11 -lstdc++ -lgcc_s -lCrunG3 -lc")
endif()

add_pdxautoserializer(testobject PortfolioPdxAuto PortfolioPdxAuto.hpp Serializable "")
add_pdxautoserializer(testobject PositionPdxAuto PositionPdxAuto.hpp Serializable "")

set( CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG "${CMAKE_BINARY_DIR}")
set( CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE "${CMAKE_BINARY_DIR}")

if (${WIN32})
  set(GFCPPCACHE "${NATIVECLIENT_LIBRARIES}/gfcppcache${CMAKE_IMPORT_LIBRARY_SUFFIX}")

  file(COPY ${NATIVECLIENT_BINARIES_DIR}/gfcppcache.dll DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
  set(RUNCPP "runcpp.bat")
else ()
  get_filename_component(GFCPPCACHE "${NATIVECLIENT_LIBRARIES}/${CMAKE_SHARED_LIBRARY_PREFIX}gfcppcache${CMAKE_SHARED_LIBRARY_SUFFIX}" ABSOLUTE)

  set(RUNCPP "runcpp.sh")
endif()

# Configure the run script
configure_file(${CMAKE_SOURCE_DIR}/../${RUNCPP}.in "${CMAKE_BINARY_DIR}/${RUNCPP}")

add_definitions(-DBUILD_TESTOBJECT)
include_directories("${NATIVECLIENT_INCLUDE_DIR}")

add_executable(HACache HACache.cpp)
target_link_libraries(HACache PUBLIC ${GFCPPCACHE})

add_executable(Exceptions Exceptions.cpp)
target_link_libraries(Exceptions PUBLIC ${GFCPPCACHE})

add_executable(BasicOperations BasicOperations.cpp)
target_link_libraries(BasicOperations PUBLIC ${GFCPPCACHE})

add_executable(DistributedSystem DistributedSystem.cpp)
target_link_libraries(DistributedSystem PUBLIC ${GFCPPCACHE})

add_executable(RefIDExample RefIDExample.cpp)
target_link_libraries(RefIDExample PUBLIC ${GFCPPCACHE})

add_executable(PoolWithEndpoints PoolWithEndpoints.cpp)
target_link_libraries(PoolWithEndpoints PUBLIC ${GFCPPCACHE})

add_executable(DataExpiration DataExpiration.cpp plugins/SimpleCacheListener.cpp)
target_link_libraries(DataExpiration PUBLIC ${GFCPPCACHE})

add_executable(LoaderListenerWriter 
  LoaderListenerWriter.cpp
  plugins/SimpleCacheLoader.cpp
  plugins/SimpleCacheListener.cpp
  plugins/SimpleCacheWriter.cpp)
target_link_libraries(LoaderListenerWriter PUBLIC ${GFCPPCACHE})

add_executable(DurableClient
  DurableClient.cpp
  plugins/DurableCacheListener.cpp)
target_link_libraries(DurableClient PUBLIC ${GFCPPCACHE})

add_executable(RegisterInterest RegisterInterest.cpp)
target_link_libraries(RegisterInterest PUBLIC ${GFCPPCACHE})

add_executable(Security Security.cpp)
target_link_libraries(Security PUBLIC ${GFCPPCACHE})

add_executable(MultiuserSecurity MultiuserSecurity.cpp)
target_link_libraries(MultiuserSecurity PUBLIC ${GFCPPCACHE})

add_executable(RemoteQuery
  RemoteQuery.cpp
  queryobjects/Portfolio.cpp
  queryobjects/Position.cpp)
target_link_libraries(RemoteQuery PUBLIC ${GFCPPCACHE})

add_executable(PoolRemoteQuery
  PoolRemoteQuery.cpp
  queryobjects/Portfolio.cpp
  queryobjects/Position.cpp)
target_link_libraries(PoolRemoteQuery PUBLIC ${GFCPPCACHE})

add_executable(CqQuery
  CqQuery.cpp
  queryobjects/Portfolio.cpp
  queryobjects/Position.cpp)
target_link_libraries(CqQuery PUBLIC ${GFCPPCACHE})

add_executable(Delta Delta.cpp)
target_link_libraries(Delta PUBLIC ${GFCPPCACHE})

add_executable(ExecuteFunctions ExecuteFunctions.cpp)
target_link_libraries(ExecuteFunctions PUBLIC ${GFCPPCACHE})

add_executable(PoolCqQuery
  PoolCqQuery.cpp
  queryobjects/Portfolio.cpp
  queryobjects/Position.cpp)
target_link_libraries(PoolCqQuery PUBLIC ${GFCPPCACHE})

add_executable(PutAllGetAllOperations PutAllGetAllOperations.cpp)
target_link_libraries(PutAllGetAllOperations PUBLIC ${GFCPPCACHE})

add_executable(Transactions Transactions.cpp)
target_link_libraries(Transactions PUBLIC ${GFCPPCACHE})

add_executable(TransactionsXA TransactionsXA.cpp)
target_link_libraries(TransactionsXA PUBLIC ${GFCPPCACHE})

add_executable(PdxInstance PdxInstance.cpp)
target_link_libraries(PdxInstance PUBLIC ${GFCPPCACHE})

add_executable(PdxSerializer PdxSerializer.cpp)
target_link_libraries(PdxSerializer PUBLIC ${GFCPPCACHE})

add_executable(PdxRemoteQuery
  PdxRemoteQuery.cpp
  queryobjects/PortfolioPdx.cpp
  queryobjects/PositionPdx.cpp)
target_link_libraries(PdxRemoteQuery PUBLIC ${GFCPPCACHE})

add_executable(PdxAutoSerializer
  PdxAutoSerializer.cpp
  queryobjects/PortfolioPdxAuto.cpp
  queryobjects/PositionPdxAuto.cpp
  queryobjects/testobject_PortfolioPdxAutoSerializable.cpp
  queryobjects/testobject_PositionPdxAutoSerializable.cpp
  )
target_link_libraries(PdxAutoSerializer PUBLIC ${GFCPPCACHE})

file(COPY ../XMLs DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ../keystore DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
