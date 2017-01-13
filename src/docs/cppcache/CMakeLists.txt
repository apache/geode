cmake_minimum_required(VERSION 3.4)
project(docs-cppcache LANGUAGES NONE)

add_dependencies(docs docs-cppcache)

find_package(Doxygen)
if(DOXYGEN_FOUND)
  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile @ONLY)
  add_custom_target(docs-cppcache
      ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Generating C++ API documentation with Doxygen" VERBATIM)
  set_target_properties(docs-cppcache PROPERTIES EXCLUDE_FROM_ALL TRUE EXCLUDE_FROM_DEFAULT_BUILD TRUE)
endif()

install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html/ DESTINATION docs/cppdocs)
