cmake_minimum_required(VERSION 3.20)
project(iceberg-cpp)

option(HAS_ARROW_CSV "Arrow built with csv" ON)
option(USE_SMHASHER "Enable SMHasher" OFF)
option(ENABLE_REST_TESTS "enable rest tests" ON)
option(BUILD_TOOLS "build binaries" ON)
option(ICEBERG_GEN "build generator" OFF)

set(CMAKE_CXX_STANDARD 20)
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-macro-redefined")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -Wno-macro-redefined")

# deps

find_package(Arrow REQUIRED PATHS ${DEPS_PATH})
find_package(absl REQUIRED)
find_package(Parquet REQUIRED PATHS ${DEPS_PATH})

# Fix up missing dependency declaration
set_property(TARGET Arrow::arrow_bundled_dependencies
  APPEND PROPERTY INTERFACE_LINK_LIBRARIES "OpenSSL::Crypto;OpenSSL::SSL"
)

set(GITLAB github.com)
set(GITLAB_SSH ssh://git@${GITLAB}:7999)
if (GIT_SSH)
  set(GITHUB_MIRROR ${GITLAB_SSH}/github-mirror)
else ()
  set(GITHUB_MIRROR https://${GITLAB}/github-mirror)
endif()

add_subdirectory(vendor)

set(INSTALL_GTEST OFF)
set(INSTALL_GMOCK OFF)
FetchContent_MakeAvailable(googletest)

if(USE_SMHASHER)
  FetchContent_MakeAvailable(smhasher)
  add_library(
    SMHasherMurmurHash3
    ${smhasher_SOURCE_DIR}/src/MurmurHash3.cpp
  )
  include_directories(${smhasher_SOURCE_DIR}/src)
  add_definitions(-DUSE_SMHASHER)
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

if (ENABLE_REST_TESTS)
  add_definitions(-DENABLE_REST_TESTS)
endif()
if (DISABLE_BROKEN_TESTS)
  add_definitions(-DDISABLE_BROKEN_TESTS)
endif()
add_subdirectory(tests)
