cmake_minimum_required (VERSION 2.8)
find_package(Java REQUIRED)
include(UseJava)
project (javaobject)

# Determine JAVA_HOME.
get_filename_component(JAVA_BIN ${Java_JAVA_EXECUTABLE} DIRECTORY)
get_filename_component(JAVA_HOME ${JAVA_BIN} DIRECTORY)

# Update the class path.
set(CMAKE_JAVA_INCLUDE_PATH ${JAVA_HOME}/lib/tools.jar)
if (GEMFIRE_HOME)
    LIST(APPEND CMAKE_JAVA_INCLUDE_PATH ${GEMFIRE_HOME}/lib/geode-core-9.0.0.jar)
else()
    LIST(APPEND CMAKE_JAVA_INCLUDE_PATH "/gemfire/lib/geode-core-9.0.0.jar")
endif()

# Compile and jar the classes.
file(GLOB_RECURSE JAVA_SOURCES "*.java")
add_jar(javaobject ${JAVA_SOURCES})
