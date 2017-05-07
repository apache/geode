cmake_minimum_required(VERSION 3.4)
project(cppcache)

file(GLOB_RECURSE SOURCES "*.cpp")

if(CMAKE_SYSTEM_NAME STREQUAL "SunOS")
# This is the root definition for platform libraries - DO NOT REMOVE
     set(PLATFORM_LIBRARIES 
            rt
            pthread
            m
            socket
            nsl
            demangle
            kstat
     )
  if (CMAKE_SYSTEM_PROCESSOR STREQUAL "sparc")
    set(CMAKE_ASM_COMPILER fbe)
    set(CMAKE_ASM_SOURCE_FILE_EXTENSIONS asm)
    set(CMAKE_ASM_COMPILE_OBJECT "<CMAKE_ASM_COMPILER> -m64 -K PIC -L -s -P -o <OBJECT> <SOURCE>")
    enable_language(ASM)
    add_library(gfcppasm OBJECT impl/hostsolaris.asm)
    set(SOURCES ${SOURCES} $<TARGET_OBJECTS:gfcppasm>)
  endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
   set(PLATFORM_LIBRARIES
        pthread
        dl
        rt
   )
endif()
if (${UNIX})
  set(PLATFORM_LIBRARIES ${PLATFORM_LIBRARIES} z)
elseif (${WIN32})
  set(PLATFORM_LIBRARIES ${PLATFORM_LIBRARIES} Dbghelp)
endif()

add_library(gfcppcache SHARED ${SOURCES} version.h)

#TODO get external project library names
target_link_libraries(gfcppcache
        PUBLIC
            ${PLATFORM_LIBRARIES}
            ${RUNTIME_LIBRARIES}
        PRIVATE
            ${ACE_STATIC_LIB}
            ${libxml2_STATIC_LIB}
)

add_dependencies(gfcppcache libxml2 ACE)

if (${BUILD_CLI})
  add_library(gfcppcache-static STATIC ${SOURCES})
  target_link_libraries(gfcppcache-static
		  PUBLIC
			  ${PLATFORM_LIBRARIES}
			  ${RUNTIME_LIBRARIES}
		  PRIVATE
			  ${ACE_STATIC_LIB}
			  ${libxml2_STATIC_LIB}
  )
  add_dependencies(gfcppcache-static libxml2 ACE)
endif()



add_definitions(-DBUILD_CPPCACHE)
#TODO move to config.h when not building shared ace
add_definitions(-D__ACE_INLINE__)
add_definitions(-DACE_AS_STATIC_LIBS)

#TODO is this correct for config.h
include_directories(${CMAKE_CURRENT_BINARY_DIR})
#TODO fix dependencies include paths
include_directories(${DEPENDENCIES_ACE_DIR}/include)
include_directories(${DEPENDENCIES_libxml2_DIR}/include/libxml2)

# TODO seems redundant
set(CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES}
        ${CMAKE_BINARY_DIR}/dependencies/ACE/ACE-prefix/include
        ${CMAKE_BINARY_DIR}/dependencies/libxml2/libxml2-prefix/include/libxml2) 

# TODO checks
include(CheckIncludeFiles)
check_include_files(malloc.h HAVE_MALLOC_H)
check_include_files("sys/param.h;sys/mount.h" HAVE_SYS_MOUNT_H)

include(CheckSymbolExists)
check_symbol_exists(SIGSTKFLT "signal.h" HAVE_SIGSTKFLT)
check_symbol_exists(SIGPWR "signal.h" HAVE_SIGPWR)

include(CheckCXXSymbolExists)
#TODO how can we do this, ACE not built yet
#check_cxx_symbol_exists(ACE::ACE_Select_Reactor "ace/config.h;ace/Select_Reactor.h" HAVE_ACE_Select_Reactor)
#check_cxx_symbol_exists(ACE::ACE_Dev_Poll_Reactor "ace/config.h;ace/Dev_Poll_Reactor.h" HAVE_ACE_Dev_Poll_Reactor)

#TODO cmake collides with gf_globals.hpp
if ("SunOS" STREQUAL ${CMAKE_SYSTEM_NAME})
  set( _SOLARIS 1 )
elseif ("Linux" STREQUAL ${CMAKE_SYSTEM_NAME} )
  set( _LINUX 1 )
elseif ("Darwin" STREQUAL ${CMAKE_SYSTEM_NAME} )
  set( _MACOSX 1 )
elseif ("Windows" STREQUAL ${CMAKE_SYSTEM_NAME} )
  set( _WIN32 1 )
endif()

#TODO cache values
set(GEMFIRE_PRODUCTNAME "GemFire Native Client")
set(GEMFIRE_BITS 64-bit)
set(GEMFIRE_BASE_VERSION 9.0.0)

find_package(Git 1.7 REQUIRED)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/version.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/version.cmake @ONLY)
add_custom_command(OUTPUT version.h
    COMMAND ${CMAKE_COMMAND} -P version.cmake
    DEPENDS ${CMAKE_SOURCE_DIR}/../.git/index ${CMAKE_CURRENT_BINARY_DIR}/version.cmake
)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

# TODO remove below after fixing module includes

# TODO has to be a better way to deal with includes
# TODO shouldn't have to copy them for sub projects
# TODO install should copy them to prefix
macro(copy_files TARGET GLOBPAT DESTINATION)
  file(GLOB_RECURSE COPY_FILES
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    ${GLOBPAT})

  foreach(FILENAME ${COPY_FILES})
    set(SRC "${CMAKE_CURRENT_SOURCE_DIR}/${FILENAME}")
    set(DST "${DESTINATION}/${FILENAME}")

    add_custom_command(
      OUTPUT ${DST}
      COMMAND ${CMAKE_COMMAND} -E copy ${SRC} ${DST}
      DEPENDS ${SRC}
      COMMENT "")

    set(${TARGET} ${${TARGET}} ${DST})
 
  endforeach(FILENAME)
endmacro(copy_files)

copy_files(INCLUDES *.h* ${CMAKE_CURRENT_BINARY_DIR}/include/gfcpp)
copy_files(INCLUDES *.inl ${CMAKE_CURRENT_BINARY_DIR}/include/gfcpp)

add_custom_target(copyCPPIncludes ALL
    DEPENDS ${INCLUDES})
