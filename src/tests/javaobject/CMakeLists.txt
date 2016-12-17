cmake_minimum_required (VERSION 3.4)
project (javaobject)

find_package(Geode REQUIRED)
find_package(Java REQUIRED)

include(UseJava)

file(GLOB_RECURSE SOURCES "*.java")

add_jar(javaobject ${SOURCES}
  INCLUDE_JARS ${Geode_CLASSPATH}
)
