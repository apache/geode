cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests.pdxautoserializerclass)

file(GLOB_RECURSE SOURCES "*.cpp")

#add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/AutoPdxTests_NestedPdxSerializable.cpp
#    COMMAND ${AUTOPDXSERIALIZER} --className=NestedPdx ${CMAKE_CURRENT_SOURCE_DIR}/AutoPdxVersioned1.hpp
#    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/AutoPdxVersioned1.hpp
#)

macro(add_pdxautoserializer SOURCES_VAR NAMESPACE CLASS HEADER FLAGS)
    set(_OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${NAMESPACE}_${CLASS}Serializable.cpp)
    set(_INPUT ${CMAKE_CURRENT_SOURCE_DIR}/${HEADER})
    add_custom_command(OUTPUT ${_OUTPUT}
        COMMAND $<TARGET_FILE:pdxautoserializer> --className=${CLASS} ${_INPUT} ${FLAGS}
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

#TODO get external project library names
target_link_libraries(pdxobject
    PUBLIC 
        gfcppcache
#transitive	${RUNTIME_LIBRARIES}
)

add_dependencies(pdxobject pdxautoserializer)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

#TODO is this correct for config.h
include_directories(${CMAKE_BINARY_DIR}/cppcache)
include_directories(${CMAKE_BINARY_DIR}/cppcache/include)
#TODO fix dependencies include paths
include_directories(${CMAKE_SOURCE_DIR}/cppcache)
include_directories(${CMAKE_SOURCE_DIR}/cppcache/impl)
#TODO very bad we include root of tests
include_directories(${CMAKE_SOURCE_DIR}/tests)

