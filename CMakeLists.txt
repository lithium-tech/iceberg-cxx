cmake_minimum_required(VERSION 3.20)
project(iceberg-cpp)

include(cmake/gtest.cmake)

find_package(Arrow REQUIRED)
find_package(absl REQUIRED)

add_subdirectory(vendor)

add_subdirectory(src)
target_include_directories(iceberg-cpp PUBLIC ${CMAKE_SOURCE_DIR})

add_subdirectory(tools)
add_subdirectory(tests)
