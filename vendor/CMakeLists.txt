set_property(DIRECTORY PROPERTY EXCLUDE_FROM_ALL 1)

if(NOT DEFINED GITHUB)
  set(GITHUB https://github.com)
endif()

include(FetchContent)

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

# Declare

FetchContent_Declare(
  avro
  GIT_REPOSITORY ${GITHUB}/apache/avro.git
  GIT_TAG 82a2bc8b034de34626e2ab8bf091234122474d50
  PATCH_COMMAND git reset --hard HEAD
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/avro/avro_metadata.patch
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/avro/avro_format.patch
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/avro/avro_missing_cstring.patch
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/avro/avro_default_value.patch
    && cp ${CMAKE_CURRENT_SOURCE_DIR}/avro/CMakeLists.txt ./
)

FetchContent_Declare(
  rapidjson
  GIT_REPOSITORY ${GITHUB}/clickhouse/rapidjson.git
  GIT_TAG 800ca2f38fc3b387271d9e1926fcfc9070222104
  PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/rapidjson/CMakeLists.txt ./
)

FetchContent_Declare(
  thrift
  GIT_REPOSITORY ${GITHUB}/apache/thrift.git
  GIT_TAG df626d768a87fe07fef215b4dde831185e6929d7 # v0.19.0
  PATCH_COMMAND git reset --hard HEAD
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/thrift/thrift-no-uuid-to-string.patch
    && cp ${CMAKE_CURRENT_SOURCE_DIR}/thrift/CMakeLists.txt ./
)

FetchContent_Declare(
  hive-metastore
  GIT_REPOSITORY ${GITHUB}/clickhouse/hive-metastore.git
  GIT_TAG 809a77d435ce218d9b000733f19489c606fc567b
  PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/hive-metastore/CMakeLists.txt ./
)

FetchContent_Declare(
  snappy
  GIT_REPOSITORY ${GITHUB}/google/snappy.git
  GIT_TAG 6af9287fbdb913f0794d0148c6aa43b58e63c8e3 # 1.2.2
  GIT_SUBMODULES ""
)

FetchContent_Declare(
  zstd
  GIT_REPOSITORY ${GITHUB}/facebook/zstd.git
  GIT_TAG 63779c798237346c2b245c546c40b72a5a5913fe # v1.5.5
  PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/zstd/CMakeLists.txt ./
)

FetchContent_Declare(
  zlib-ng
  GIT_REPOSITORY ${GITHUB}/clickhouse/zlib-ng.git
  GIT_TAG ace95551e260bbd6a8c5af31a905d4f9ee742735
  PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/zlib-ng/CMakeLists.txt ./
)

FetchContent_Declare(
  abseil
  GIT_REPOSITORY ${GITHUB}/abseil/abseil-cpp.git
  GIT_TAG lts_2024_07_22
)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY ${GITHUB}/google/googletest.git
  GIT_TAG 52eb8108c5bdec04579160ae17225d66034bd723 # v1.17.0
)

FetchContent_Declare(
  croaring
  GIT_REPOSITORY ${GITHUB}/roaringbitmap/croaring.git
  GIT_TAG 7c1fd0355438299a902194193f56f814767a5472 # v2.1.2
  PATCH_COMMAND ./amalgamation.sh
    && cp ${CMAKE_CURRENT_SOURCE_DIR}/croaring/CMakeLists.txt ./
)

if (ICECXX_STATISTICS)
  FetchContent_Declare(
    datasketches-cpp
    GIT_REPOSITORY ${GITHUB}/apache/datasketches-cpp.git
    GIT_TAG de8553ba372e618382c2e7b44b0ffc9422b9458c # 5.2.0
    PATCH_COMMAND git reset --hard HEAD
      && git apply ${CMAKE_CURRENT_SOURCE_DIR}/datasketches-cpp/count.patch
  )
endif()

FetchContent_Declare(
  smhasher
  GIT_REPOSITORY ${GITHUB}/aappleby/smhasher.git
  GIT_TAG 0ff96f7835817a27d0487325b6c16033e2992eb5
)

FetchContent_Declare(
  c-ares
  GIT_REPOSITORY ${GITHUB}/c-ares/c-ares.git
  GIT_TAG 6360e96b5cf8e5980c887ce58ef727e53d77243a # 1.19.1
  PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/c-ares/CMakeLists.txt ./
    && cp -R ${CMAKE_CURRENT_SOURCE_DIR}/c-ares/linux ./
    && cp -R ${CMAKE_CURRENT_SOURCE_DIR}/c-ares/darwin ./
)

FetchContent_Declare(
  cpr
  GIT_REPOSITORY ${GITHUB}/libcpr/cpr.git
  GIT_TAG dd967cb48ea6bcbad9f1da5ada0db8ac0d532c06 # 1.11.2
  PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/cpr/CMakeLists.txt ./
)

FetchContent_Declare(
  curl
  GIT_REPOSITORY ${GITHUB}/curl/curl.git
  GIT_TAG eebb1c1c15d24b8cf103499f7d96d9eb6994f837 # curl-8_12_1
  PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/curl/CMakeLists.txt ./
    && cp ${CMAKE_CURRENT_SOURCE_DIR}/curl/curl_config.h ./
)

if (DEFINED ENV{ARROW_DEPS})
  set(ARROW_DEPS_DIR "$ENV{ARROW_DEPS}")
else ()
  set(ARROW_DEPS_DIR ${CMAKE_BINARY_DIR}/arrow-thirdparty)
endif ()

set(ENV{ARROW_ABSL_URL}                "${ARROW_DEPS_DIR}/absl-20211102.0.tar.gz")
set(ENV{ARROW_AWS_C_AUTH_URL}          "${ARROW_DEPS_DIR}/aws-c-auth-v0.6.22.tar.gz")
set(ENV{ARROW_AWS_C_CAL_URL}           "${ARROW_DEPS_DIR}/aws-c-cal-v0.5.20.tar.gz")
set(ENV{ARROW_AWS_C_COMMON_URL}        "${ARROW_DEPS_DIR}/aws-c-common-v0.8.9.tar.gz")
set(ENV{ARROW_AWS_C_COMPRESSION_URL}   "${ARROW_DEPS_DIR}/aws-c-compression-v0.2.16.tar.gz")
set(ENV{ARROW_AWS_C_EVENT_STREAM_URL}  "${ARROW_DEPS_DIR}/aws-c-event-stream-v0.2.18.tar.gz")
set(ENV{ARROW_AWS_C_HTTP_URL}          "${ARROW_DEPS_DIR}/aws-c-http-v0.7.3.tar.gz")
set(ENV{ARROW_AWS_C_IO_URL}            "${ARROW_DEPS_DIR}/aws-c-io-v0.13.14.tar.gz")
set(ENV{ARROW_AWS_C_MQTT_URL}          "${ARROW_DEPS_DIR}/aws-c-mqtt-v0.8.4.tar.gz")
set(ENV{ARROW_AWS_C_S3_URL}            "${ARROW_DEPS_DIR}/aws-c-s3-v0.2.3.tar.gz")
set(ENV{ARROW_AWS_C_SDKUTILS_URL}      "${ARROW_DEPS_DIR}/aws-c-sdkutils-v0.1.6.tar.gz")
set(ENV{ARROW_AWS_CHECKSUMS_URL}       "${ARROW_DEPS_DIR}/aws-checksums-v0.1.13.tar.gz")
set(ENV{ARROW_AWS_CRT_CPP_URL}         "${ARROW_DEPS_DIR}/aws-crt-cpp-v0.18.16.tar.gz")
set(ENV{ARROW_AWS_LC_URL}              "${ARROW_DEPS_DIR}/aws-lc-v1.3.0.tar.gz")
set(ENV{ARROW_AWSSDK_URL}              "${ARROW_DEPS_DIR}/aws-sdk-cpp-1.10.55.tar.gz")
set(ENV{ARROW_BOOST_URL}               "${ARROW_DEPS_DIR}/boost-1.81.0.tar.gz")
set(ENV{ARROW_BROTLI_URL}              "${ARROW_DEPS_DIR}/brotli-v1.0.9.tar.gz")
set(ENV{ARROW_BZIP2_URL}               "${ARROW_DEPS_DIR}/bzip2-1.0.8.tar.gz")
set(ENV{ARROW_CARES_URL}               "${ARROW_DEPS_DIR}/cares-1.17.2.tar.gz")
set(ENV{ARROW_CRC32C_URL}              "${ARROW_DEPS_DIR}/crc32c-1.1.2.tar.gz")
set(ENV{ARROW_GBENCHMARK_URL}          "${ARROW_DEPS_DIR}/gbenchmark-v1.7.1.tar.gz")
set(ENV{ARROW_GFLAGS_URL}              "${ARROW_DEPS_DIR}/gflags-v2.2.2.tar.gz")
set(ENV{ARROW_GLOG_URL}                "${ARROW_DEPS_DIR}/glog-v0.5.0.tar.gz")
set(ENV{ARROW_GOOGLE_CLOUD_CPP_URL}    "${ARROW_DEPS_DIR}/google-cloud-cpp-v2.12.0.tar.gz")
set(ENV{ARROW_GRPC_URL}                "${ARROW_DEPS_DIR}/grpc-v1.46.3.tar.gz")
set(ENV{ARROW_GTEST_URL}               "${ARROW_DEPS_DIR}/gtest-1.11.0.tar.gz")
set(ENV{ARROW_JEMALLOC_URL}            "${ARROW_DEPS_DIR}/jemalloc-5.3.0.tar.bz2")
set(ENV{ARROW_LZ4_URL}                 "${ARROW_DEPS_DIR}/lz4-v1.9.4.tar.gz")
set(ENV{ARROW_MIMALLOC_URL}            "${ARROW_DEPS_DIR}/mimalloc-v2.0.6.tar.gz")
set(ENV{ARROW_NLOHMANN_JSON_URL}       "${ARROW_DEPS_DIR}/nlohmann-json-v3.10.5.tar.gz")
set(ENV{ARROW_OPENTELEMETRY_URL}       "${ARROW_DEPS_DIR}/opentelemetry-cpp-v1.8.1.tar.gz")
set(ENV{ARROW_OPENTELEMETRY_PROTO_URL} "${ARROW_DEPS_DIR}/opentelemetry-proto-v0.17.0.tar.gz")
set(ENV{ARROW_ORC_URL}                 "${ARROW_DEPS_DIR}/orc-1.9.2.tar.gz")
set(ENV{ARROW_PROTOBUF_URL}            "${ARROW_DEPS_DIR}/protobuf-v21.3.tar.gz")
set(ENV{ARROW_RAPIDJSON_URL}           "${ARROW_DEPS_DIR}/rapidjson-232389d4f1012dddec4ef84861face2d2ba85709.tar.gz")
set(ENV{ARROW_RE2_URL}                 "${ARROW_DEPS_DIR}/re2-2022-06-01.tar.gz")
set(ENV{ARROW_S2N_TLS_URL}             "${ARROW_DEPS_DIR}/s2n-v1.3.35.tar.gz")
set(ENV{ARROW_SNAPPY_URL}              "${ARROW_DEPS_DIR}/snappy-1.1.10.tar.gz")
set(ENV{ARROW_THRIFT_URL}              "${ARROW_DEPS_DIR}/thrift-0.16.0.tar.gz")
set(ENV{ARROW_UCX_URL}                 "${ARROW_DEPS_DIR}/ucx-1.12.1.tar.gz")
set(ENV{ARROW_UTF8PROC_URL}            "${ARROW_DEPS_DIR}/utf8proc-v2.7.0.tar.gz")
set(ENV{ARROW_XSIMD_URL}               "${ARROW_DEPS_DIR}/xsimd-9.0.1.tar.gz")
set(ENV{ARROW_ZLIB_URL}                "${ARROW_DEPS_DIR}/zlib-1.3.1.tar.gz")
set(ENV{ARROW_ZSTD_URL}                "${ARROW_DEPS_DIR}/zstd-1.5.5.tar.gz")

FetchContent_Declare(
  arrow
  GIT_REPOSITORY ${GITHUB}/apache/arrow.git
  GIT_TAG e03105efc38edca4ca429bf967a17b4d0fbebe40 # maint-15.0.2
  SOURCE_SUBDIR cpp
  GIT_SUBMODULES ""
  PATCH_COMMAND git reset --hard HEAD
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/arrow/arrow-fix-snappy-empty-column.patch
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/arrow/arrow_ensure_xsimd.patch
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/arrow/fix_c-ares_url.patch
)

FetchContent_Declare(
  boost
  URL ${ARROW_DEPS_DIR}/boost-1.81.0.tar.gz
  URL_HASH MD5=2a34f49b3475a7e1cf2b4153760e499c
  PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/boost/CMakeLists.txt ./
    && rm -R ./boost/iostreams/ ./boost/random/
    && cp -R ${CMAKE_CURRENT_SOURCE_DIR}/boost/boost/iostreams ./boost/
    && cp -R ${CMAKE_CURRENT_SOURCE_DIR}/boost/boost/random ./boost/
)

# MakeAvailable

set(BOOST_ENABLE_CMAKE ON CACHE BOOL "")
FetchContent_MakeAvailable(boost)

if(ICECXX_BUILD_ARROW)
  set(ARROW_DEPENDENCY_SOURCE BUNDLED CACHE STRING "")
  set(ARROW_SIMD_LEVEL DEFAULT CACHE STRING "") # xsimd
  set(ARROW_BUILD_STATIC ON CACHE BOOL "")
  set(ARROW_BUILD_SHARED OFF CACHE BOOL "")
  set(ARROW_NO_DEPRECATED_API ON CACHE BOOL "")
  set(ARROW_LLVM_USE_SHARED OFF CACHE BOOL "")
  set(ARROW_FILESYSTEM ON CACHE BOOL "")
  set(ARROW_PARQUET ON CACHE BOOL "")
  set(ARROW_IPC ON CACHE BOOL "")
  set(ARROW_S3 ON CACHE BOOL "")
  set(ARROW_WITH_SNAPPY ON CACHE BOOL "")
  set(ARROW_WITH_LZ4 ON CACHE BOOL "")
  set(ARROW_WITH_ZLIB ON CACHE BOOL "")
  set(ARROW_WITH_ZSTD ON CACHE BOOL "")
  set(ARROW_CSV ON CACHE BOOL "")
  set(ARROW_WITH_RE2 ON CACHE BOOL "")
  set(ARROW_WITH_RAPIDJSON ON CACHE BOOL "")
  set(ARROW_WITH_UTF8PROC ON CACHE BOOL "")
  set(ARROW_COMPUTE ${ICECXX_COMPUTE} CACHE BOOL "")
  if (ICECXX_USE_GANDIVA)
    set(ARROW_GANDIVA ON CACHE BOOL "")
    set(ARROW_GANDIVA_STATIC_LIBSTDCPP ON CACHE BOOL "")
    set(ARROW_LLVM_USE_SHARED OFF CACHE BOOL "")
  endif()

  FetchContent_MakeAvailable(arrow)

  add_library(Arrow::arrow_static ALIAS arrow_static)
  add_library(Parquet::parquet_static ALIAS parquet_static)
  if (ICECXX_USE_GANDIVA)
    add_library(Gandiva::gandiva_static ALIAS gandiva_static)
  endif()
endif()

FetchContent_MakeAvailable(avro)

set(ROARING_BUILD_STATIC OFF CACHE BOOL "")
set(ENABLE_ROARING_TESTS OFF CACHE BOOL "")
set(ROARING_USE_CPM OFF CACHE BOOL "")
FetchContent_MakeAvailable(croaring)

set(BUILD_TESTS OFF CACHE BOOL "")
if (ICECXX_STATISTICS)
  FetchContent_MakeAvailable(datasketches-cpp)
endif()

FetchContent_MakeAvailable(hive-metastore)
FetchContent_MakeAvailable(rapidjson)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(OS_LINUX ON CACHE BOOL "")
endif()
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

if(ICECXX_BUILD_ABSEIL)
  set_property(
    DIRECTORY ${abseil_SOURCE_DIR}
    APPEND
    PROPERTY COMPILE_OPTIONS -Wno-deprecated-builtins
  )
  set(ABSL_USE_EXTERNAL_GOOGLETEST ON CACHE BOOL "")
  set(BUILD_TESTING OFF CACHE BOOL "")
  set(ABSL_ENABLE_INSTALL OFF CACHE BOOL "")
  FetchContent_MakeAvailable(abseil)
endif()

if(ICECXX_USE_REST)
  set(CARES_STATIC ON CACHE BOOL "")
  set(CARES_SHARED OFF CACHE BOOL "")
  set(CARES_BUILD_TOOLS OFF CACHE BOOL "")
  FetchContent_MakeAvailable(c-ares)

  FetchContent_MakeAvailable(cpr)
  FetchContent_MakeAvailable(curl)
endif()

if(ICECXX_USE_SMHASHER)
  FetchContent_MakeAvailable(smhasher)
  add_library(
    SMHasherMurmurHash3
    ${smhasher_SOURCE_DIR}/src/MurmurHash3.cpp
  )
  target_include_directories(SMHasherMurmurHash3 INTERFACE ${smhasher_SOURCE_DIR}/src)
endif()
