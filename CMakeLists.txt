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

# feature options
option_v(ICECXX_BUILD_TOOLS "enable & build tools" ON)
option_v(ICECXX_GENERATOR "enable & build generator" ON)
option_v(ICECXX_USE_REST "enable rest catalog support" OFF)
option_v(ICECXX_STATISTICS "enable datasketches based statistics" OFF)
option_v(ICECXX_COMPUTE "enable compute based on Arrow.Compute" ON)
# deps options
option_v(ICECXX_BUILD_ARROW "build Apache Arrow (use system's one otherwise)" ON)
option_v(ICECXX_BUILD_ABSEIL "build abseil (use system's one otherwise)" ON)
option_v(ICECXX_USE_SMHASHER "use MurmurHash3 from SMHasher" ON)

set(CMAKE_CXX_STANDARD 20)

if(NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "None")
  set(CMAKE_BUILD_TYPE "Release")
  message(STATUS "CMAKE_BUILD_TYPE is not set, set to default = ${CMAKE_BUILD_TYPE}")
endif()

# deps

set_directory_properties(PROPERTIES COMPILE_OPTIONS -Wno-deprecated-declarations)

find_package(OpenSSL REQUIRED)

if(NOT ICECXX_BUILD_ABSEIL)
  find_package(absl REQUIRED)
endif()

add_subdirectory(vendor)

if(ICECXX_BUILD_ARROW)
  FetchContent_GetProperties(
    arrow
    SOURCE_DIR ARROW_SOURCE_DIR
    BINARY_DIR ARROW_BINARY_DIR
  )
  include_directories(
    ${ARROW_SOURCE_DIR}/cpp/src
    ${ARROW_BINARY_DIR}/src
  )
else()
  find_package(Arrow REQUIRED PATHS ${DEPS_PATH})
  find_package(Parquet REQUIRED PATHS ${DEPS_PATH})
  find_package(Gandiva REQUIRED PATHS ${DEPS_PATH})

  # Fix up missing dependency declaration
  set_property(TARGET Arrow::arrow_bundled_dependencies
    APPEND PROPERTY INTERFACE_LINK_LIBRARIES "OpenSSL::Crypto;OpenSSL::SSL"
  )
endif()

# libs/bins

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(ICEBERG_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
include_directories(
  ${ICEBERG_SOURCE_DIR}
)

add_subdirectory(iceberg)

if (ICECXX_BUILD_TOOLS)
  add_subdirectory(tools)
endif()
if (ICECXX_STATISTICS)
  add_subdirectory(stats)
endif()
if (ICECXX_GENERATOR)
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
