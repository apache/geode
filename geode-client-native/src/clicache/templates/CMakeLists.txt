cmake_minimum_required(VERSION 3.4)
project(nativeclient.clicache.templates)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Templates.csproj.in ${CMAKE_BINARY_DIR}/clicache/templates/Templates.csproj)  

include_external_msproject(
      Templates   ${CMAKE_BINARY_DIR}/clicache/templates/Templates.csproj
      TYPE FAE04EC0-301F-11D3-BF4B-00C04F79EFBC)