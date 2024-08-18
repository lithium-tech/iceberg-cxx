cmake_minimum_required(VERSION 3.20)
project(iceberg-cpp)

set(ICEBERG_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})

include(cmake/gtest.cmake)

find_package(Arrow REQUIRED)
find_package(absl REQUIRED)
find_package(Parquet REQUIRED)

include_directories(
  ${ICEBERG_SOURCE_DIR}
)

add_subdirectory(vendor)

add_subdirectory(iceberg)

add_subdirectory(tools)
add_subdirectory(tests)

add_library(iceberg INTERFACE)
target_link_libraries(iceberg INTERFACE
  iceberg-cpp
  iceberg-tools
)
target_include_directories(iceberg SYSTEM INTERFACE
  ${ICEBERG_SOURCE_DIR}
)
