option(HAS_ARROW_CSV "Arrow built with csv" ON)

cmake_minimum_required(VERSION 3.20)
project(iceberg-cpp)

set(ICEBERG_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(cmake/gtest.cmake)

find_package(Arrow REQUIRED PATHS ${DEPS_PATH})
find_package(absl REQUIRED)
find_package(Parquet REQUIRED PATHS ${DEPS_PATH})

# Fix up missing dependency declaration
set_property(TARGET Arrow::arrow_bundled_dependencies
  APPEND PROPERTY INTERFACE_LINK_LIBRARIES "OpenSSL::Crypto;OpenSSL::SSL"
)
option(USE_SMHASHER "Enable SMHasher" OFF)
if(USE_SMHASHER)
    add_definitions(-DUSE_SMHASHER)
endif()

set(CMAKE_CXX_STANDARD 20)

include_directories(
  ${ICEBERG_SOURCE_DIR}
)

add_subdirectory(vendor)

add_subdirectory(iceberg)

if (ICEBERG_STATISTICS)
  add_subdirectory(stats)
endif()

if (ICEBERG_GEN)
  add_subdirectory(gen)
endif()
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
