cmake_minimum_required(VERSION 3.4)
project(nativeclient.plugins.SQLiteCLI)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/SQLiteCLI.csproj.in ${CMAKE_BINARY_DIR}/plugins/SQLiteCLI/SQLiteCLI.csproj)  
include_external_msproject(
      SQLiteCLI  ${CMAKE_BINARY_DIR}/plugins/SQLiteCLI/SQLiteCLI.csproj
      TYPE FAE04EC0-301F-11D3-BF4B-00C04F79EFBC)
