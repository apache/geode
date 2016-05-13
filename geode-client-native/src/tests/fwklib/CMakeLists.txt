cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests.fwklib)

file(GLOB_RECURSE SOURCES "*.cpp")

add_library(framework STATIC ${SOURCES})
set_target_properties(framework PROPERTIES FOLDER test)

#TODO get external project library names
target_link_libraries(framework
	PRIVATE
            ${ACE_STATIC_LIB}
        PUBLIC 
            gfcppcache
            ${xerces-c_SHARED_LIB}
# transitive            ${PLATFORM_LIBRARIES}
# transitive            ${RUNTIME_LIBRARIES}
)

#TODO move to config.h when not building shared ace
add_definitions(-D__ACE_INLINE__)
add_definitions(-DACE_AS_STATIC_LIBS)

# Add PIC flags to link later with shared library
add_definitions(${CMAKE_CXX_COMPILE_OPTIONS_PIC})

add_dependencies(framework ACE xerces-c)

#TODO is this correct for config.h
include_directories(${CMAKE_BINARY_DIR}/cppcache)
#TODO fix dependencies include paths
include_directories(${CMAKE_SOURCE_DIR}/cppcache)
include_directories(${CMAKE_SOURCE_DIR}/cppcache/impl)
#TODO very bad we include root of tests
include_directories(${CMAKE_SOURCE_DIR}/tests)
include_directories(${DEPENDENCIES_ACE_DIR}/include)
include_directories(${DEPENDENCIES_xerces-c_DIR}/include)

