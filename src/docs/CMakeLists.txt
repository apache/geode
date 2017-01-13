cmake_minimum_required(VERSION 3.4)
project(docs LANGUAGES NONE)

# The install built-in-target depends on the docs target to
# produce the actual documentation files it install but,
# due to a bug in CMake, there is no programmatic way to
# enforce this dependency.
add_custom_target(docs)
set_target_properties(docs PROPERTIES EXCLUDE_FROM_ALL TRUE EXCLUDE_FROM_DEFAULT_BUILD TRUE)

add_subdirectory(cppcache)
if (${BUILD_CLI})
  add_subdirectory(clicache)
  install(FILES win_index.html RENAME index.html DESTINATION docs)
else()
  install(FILES unix_index.html RENAME index.html DESTINATION docs)
endif()

install(FILES support.html DESTINATION docs)
install(FILES copyright.html DESTINATION docs)
install(FILES DocIndex.css DESTINATION docs)
install(FILES PIVOTAL_GemFire_190x81.png DESTINATION docs)

