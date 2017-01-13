cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests.testobject)

file(GLOB_RECURSE SOURCES "*.cpp")

add_definitions(-DBUILD_TESTOBJECT)

if (APPLE)
    set(DYNAMIC_LIBRARY_PATH DYLD_LIBRARY_PATH=$<TARGET_LINKER_FILE_DIR:gfcppcache>)
elseif(UNIX)
    set(DYNAMIC_LIBRARY_PATH LD_LIBRARY_PATH=$<TARGET_LINKER_FILE_DIR:gfcppcache>)
endif()

macro(add_pdxautoserializer SOURCES_VAR NAMESPACE CLASS HEADER SUFFIX CLASSNAMESTR)
    set(_OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${NAMESPACE}_${CLASS}${SUFFIX}.cpp)
    set(_INPUT ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
    add_custom_command(OUTPUT ${_OUTPUT}
        COMMAND ${DYNAMIC_LIBRARY_PATH} $<TARGET_FILE:pdxautoserializer> --className=${CLASS} --suffix=${SUFFIX} --classNameStr=${CLASSNAMESTR} ${_INPUT} 
        DEPENDS ${_INPUT}
    )
    set(${SOURCES_VAR} ${${SOURCES_VAR}} ${_OUTPUT})
endmacro()
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_pdxautoserializer(SOURCES PdxAutoTests PdxAutoMegaType PdxAutoMegaType.hpp Serializable "")

add_pdxautoserializer(SOURCES PdxTestsAuto CharTypes PdxTypeWithAuto.hpp _UTImpl "")
add_pdxautoserializer(SOURCES PdxTestsAuto PdxType PdxTypeWithAuto.hpp _UTImpl PdxType:PdxTests.PdxTestsWithAuto)
add_pdxautoserializer(SOURCES PdxTestsAuto Address PdxTypeWithAuto.hpp _UTImpl Address:PdxTests.Address)
add_pdxautoserializer(SOURCES PdxTestsAuto Child PdxTypeWithAuto.hpp _UTImpl "")

add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes1 VariousPdxTypesWithAuto.hpp _UTImpl "")
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes2 VariousPdxTypesWithAuto.hpp _UTImpl "")
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes3 VariousPdxTypesWithAuto.hpp _UTImpl "")
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes4 VariousPdxTypesWithAuto.hpp _UTImpl "")
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes5 VariousPdxTypesWithAuto.hpp _UTImpl "")
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes6 VariousPdxTypesWithAuto.hpp _UTImpl "")
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes7 VariousPdxTypesWithAuto.hpp _UTImpl "")
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes8 VariousPdxTypesWithAuto.hpp _UTImpl "")
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes9 VariousPdxTypesWithAuto.hpp _UTImpl "")
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes10 VariousPdxTypesWithAuto.hpp _UTImpl "")
add_pdxautoserializer(SOURCES PdxTestsAuto NestedPdx VariousPdxTypesWithAuto.hpp _UTImpl "")

add_pdxautoserializer(SOURCES PdxTestsAuto PdxType1V1 PdxClassV1WithAuto.hpp _UTImpl PdxType1V1:PdxTestsAuto.PdxType1V1)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxType2V1 PdxClassV1WithAuto.hpp _UTImpl PdxType2V1:PdxTestsAuto.PdxType2V1)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxType3V1 PdxClassV1WithAuto.hpp _UTImpl PdxType3V1:PdxTestsAuto.PdxType3V1)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypesV1R1 PdxClassV1WithAuto.hpp _UTImpl PdxTypesV1R1:PdxTestsAuto.PdxTypesV1R1)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypesV1R2 PdxClassV1WithAuto.hpp _UTImpl PdxTypesV1R2:PdxTestsAuto.PdxTypesV1R2)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypesIgnoreUnreadFieldsV1 PdxClassV1WithAuto.hpp _UTImpl dxTypesIgnoreUnreadFieldsV1:PdxTestsAuto.PdxTypesIgnoreUnreadFieldsV1)
add_pdxautoserializer(SOURCES PdxTestsAuto TestEqualsV1 PdxClassV1WithAuto.hpp _UTImpl TestEqualsV1:PdxTestsAuto.TestEqualsV1)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxVersionedV1 PdxClassV1WithAuto.hpp _UTImpl PdxVersionedV1:PdxTestsAuto.PdxVersionedV1)

add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes1V2 PdxClassV2WithAuto.hpp _UTImpl PdxTypes1V2:PdxTestsAuto.PdxType1V1)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes2V2 PdxClassV2WithAuto.hpp _UTImpl PdxTypes2V2:PdxTestsAuto.PdxType2V1)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypes3V2 PdxClassV2WithAuto.hpp _UTImpl PdxTypes3V2:PdxTestsAuto.PdxType3V1)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypesR1V2 PdxClassV2WithAuto.hpp _UTImpl PdxTypesR1V2:PdxTestsAuto.PdxTypesV1R1)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypesR2V2 PdxClassV2WithAuto.hpp _UTImpl PdxTypesR2V2:PdxTestsAuto.PdxTypesV1R2)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxTypesIgnoreUnreadFieldsV2 PdxClassV2WithAuto.hpp _UTImpl dxTypesIgnoreUnreadFieldsV2:PdxTestsAuto.PdxTypesIgnoreUnreadFieldsV1)
add_pdxautoserializer(SOURCES PdxTestsAuto PdxVersionedV2 PdxClassV2WithAuto.hpp _UTImpl PdxVersionedV2:PdxTestsAuto.PdxVersionedV1)

add_library(testobject SHARED ${SOURCES})
set_target_properties(testobject PROPERTIES FOLDER test)

target_link_libraries(testobject
  PUBLIC 
    gfcppcache
    fwk
    pdxobject
    c++11
  PRIVATE
    ACE
)


#TODO very bad we include root of tests
include_directories(${CMAKE_SOURCE_DIR}/tests/cpp)
