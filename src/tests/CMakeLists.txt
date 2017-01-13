cmake_minimum_required(VERSION 3.4)
project(nativeclient.tests)

add_subdirectory(cpp)
add_subdirectory(javaobject)

if (${BUILD_CLI})
add_subdirectory(cli)
endif()
