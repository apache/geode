cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests.pdxautoserializerclass)

file(GLOB_RECURSE SOURCES "*.cpp")

if (APPLE)
    set(DYNAMIC_LIBRARY_PATH DYLD_LIBRARY_PATH=$<TARGET_LINKER_FILE_DIR:gfcppcache>)
elseif(UNIX)
    set(DYNAMIC_LIBRARY_PATH LD_LIBRARY_PATH=$<TARGET_LINKER_FILE_DIR:gfcppcache>)
endif()

macro(add_pdxautoserializer SOURCES_VAR NAMESPACE CLASS HEADER FLAGS)
    set(_OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${NAMESPACE}_${CLASS}Serializable.cpp)
    set(_INPUT ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
    add_custom_command(OUTPUT ${_OUTPUT}
        COMMAND ${DYNAMIC_LIBRARY_PATH} $<TARGET_FILE:pdxautoserializer> --className=${CLASS} ${_INPUT} ${FLAGS}
        DEPENDS ${_INPUT}
    )
    set(${SOURCES_VAR} ${${SOURCES_VAR}} ${_OUTPUT})
endmacro()

add_pdxautoserializer(SOURCES AutoPdxTests NestedPdx AutoPdxVersioned1.hpp "")
add_pdxautoserializer(SOURCES AutoPdxTests PdxTypes1 AutoPdxVersioned1.hpp "")
add_pdxautoserializer(SOURCES AutoPdxTests PdxTypes2 AutoPdxVersioned1.hpp "")
add_pdxautoserializer(SOURCES AutoPdxTests PortfolioPdx PortfolioPdx.hpp "")
add_pdxautoserializer(SOURCES AutoPdxTests PositionPdx PositionPdx.hpp "")
add_pdxautoserializer(SOURCES AutoPdxTests AutoPdxVersioned1 AutoPdxVersioned1.hpp --classNameStr=AutoPdxVersioned1:PdxTests.PdxVersioned)
add_pdxautoserializer(SOURCES AutoPdxTests AutoPdxVersioned2 AutoPdxVersioned2.hpp --classNameStr=AutoPdxVersioned2:PdxTests.PdxVersioned)

add_library(pdxobject SHARED ${SOURCES})
set_target_properties(pdxobject PROPERTIES FOLDER test)

add_definitions(-DBUILD_TESTOBJECT)

target_link_libraries(pdxobject
  PUBLIC 
    gfcppcache
    c++11
)

add_dependencies(pdxobject pdxautoserializer)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

