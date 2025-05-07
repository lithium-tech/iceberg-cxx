cmake_minimum_required(VERSION 3.20)
project(iceberg-cxx)
cmake_policy(SET CMP0077 NEW)

macro(option_v OPTION_NAME OPTION_TEXT OPTION_DEFAULT)
  option(${OPTION_NAME} ${OPTION_TEXT} ${OPTION_DEFAULT})
  if(${OPTION_NAME})
    add_definitions(-D${OPTION_NAME})
  endif()
  message(STATUS "  ${OPTION_NAME}: ${${OPTION_NAME}}")
endmacro()

option_v(HAS_ARROW_CSV "Arrow built with csv" ON)
option_v(USE_SMHASHER "Enable SMHasher" OFF)
option_v(BUILD_TOOLS "build binaries" ON)
option_v(ICEBERG_GEN "build generator" ON)
option_v(USE_REST "enable rest building" ON)
option_v(REST_TESTS "enable rest tests" ON)
option_v(BUILD_ABSEIL "build abseil (use system's one if not set)" OFF)

set(CMAKE_CXX_STANDARD 20)

# deps

find_package(Arrow REQUIRED PATHS ${DEPS_PATH})
find_package(Parquet REQUIRED PATHS ${DEPS_PATH})

# Fix up missing dependency declaration
set_property(TARGET Arrow::arrow_bundled_dependencies
  APPEND PROPERTY INTERFACE_LINK_LIBRARIES "OpenSSL::Crypto;OpenSSL::SSL"
)

set_directory_properties(PROPERTIES COMPILE_OPTIONS -Wno-deprecated-declarations)

add_subdirectory(vendor)

FetchContent_MakeAvailable(avro)

set(ROARING_BUILD_STATIC OFF CACHE BOOL "")
set(ENABLE_ROARING_TESTS OFF CACHE BOOL "")
set(ROARING_USE_CPM OFF CACHE BOOL "")
FetchContent_MakeAvailable(croaring)

set(BUILD_TESTS OFF CACHE BOOL "")
FetchContent_MakeAvailable(datasketches-cpp)

FetchContent_MakeAvailable(hive-metastore)
FetchContent_MakeAvailable(rapidjson)
FetchContent_MakeAvailable(thrift)
FetchContent_MakeAvailable(zstd)

set(INSTALL_GTEST OFF CACHE BOOL "")
set(INSTALL_GMOCK OFF CACHE BOOL "")
FetchContent_MakeAvailable(googletest)

set(SNAPPY_BUILD_TESTS OFF CACHE BOOL "")
set(SNAPPY_BUILD_BENCHMARKS OFF CACHE BOOL "")
FetchContent_MakeAvailable(snappy)

set(ZLIB_ENABLE_TESTS OFF CACHE BOOL "")
set(ZLIBNG_ENABLE_TESTS OFF CACHE BOOL "")
set(WITH_GTEST OFF CACHE BOOL "")
FetchContent_MakeAvailable(zlib-ng)

if(BUILD_ABSEIL)
  FetchContent_MakeAvailable(abseil)
else()
  find_package(absl REQUIRED)
endif()

if(USE_REST)
  set(CARES_STATIC ON CACHE BOOL "")
  set(CARES_SHARED OFF CACHE BOOL "")
  set(CARES_BUILD_TOOLS OFF CACHE BOOL "")
  FetchContent_MakeAvailable(c-ares)

  FetchContent_MakeAvailable(cpr)
  FetchContent_MakeAvailable(curl)
endif()

if(USE_SMHASHER)
  FetchContent_MakeAvailable(smhasher)
  add_library(
    SMHasherMurmurHash3
    ${smhasher_SOURCE_DIR}/src/MurmurHash3.cpp
  )
  include_directories(${smhasher_SOURCE_DIR}/src)
endif()

# libs/bins

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(ICEBERG_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
include_directories(
  ${ICEBERG_SOURCE_DIR}
)

add_subdirectory(iceberg)

if (BUILD_TOOLS)
  add_subdirectory(tools)
endif()
if (ICEBERG_STATISTICS)
  add_subdirectory(stats)
endif()
if (ICEBERG_GEN AND HAS_ARROW_CSV)
  add_subdirectory(gen)
endif()

add_library(iceberg INTERFACE)
target_link_libraries(iceberg INTERFACE
  iceberg-cpp
  iceberg-tools
)
target_include_directories(iceberg SYSTEM INTERFACE
  ${ICEBERG_SOURCE_DIR}
)

# tests

add_subdirectory(tests)
