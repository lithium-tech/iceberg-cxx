set_property(DIRECTORY PROPERTY EXCLUDE_FROM_ALL 1)
set_directory_properties(PROPERTIES COMPILE_OPTIONS "-Wno-macro-redefined")

if(GITLAB_URL)
  if (GIT_HTTPS)
    set(GITLAB_URL https://${GITLAB})
  else()
    set(GITLAB_URL ssh://git@${GITLAB}:7999)
  endif()
  set(GITHUB ${GITLAB_URL}/github-mirror)
else()
  set(GITHUB https://github.com)
endif()

include(FetchContent)

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

FetchContent_Declare(
  avro
  GIT_REPOSITORY ${GITHUB}/clickhouse/avro.git
  GIT_TAG 545e7002683cbc2198164d93088ac8e4955b4628
  PATCH_COMMAND git reset --hard HEAD
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/avro/avro.patch
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/avro/avro_metadata.patch
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
  PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/thrift/CMakeLists.txt ./
)

FetchContent_Declare(
  hive-metastore
  GIT_REPOSITORY ${GITHUB}/clickhouse/hive-metastore
  GIT_TAG 809a77d435ce218d9b000733f19489c606fc567b
  PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/hive-metastore/CMakeLists.txt ./
)

FetchContent_Declare(
  snappy
  GIT_REPOSITORY ${GITHUB}/google/snappy.git
  GIT_TAG 6af9287fbdb913f0794d0148c6aa43b58e63c8e3 # 1.2.2
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
  GIT_TAG 053d842fa6c55ee103057b3c325b1923fd5d67d2 # lts_2022_06_23
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

FetchContent_Declare(
  datasketches-cpp
  GIT_REPOSITORY ${GITHUB}/apache/datasketches-cpp.git
  GIT_TAG de8553ba372e618382c2e7b44b0ffc9422b9458c # 5.2.0
)

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

# TODO: FetchContent
add_subdirectory(boost EXCLUDE_FROM_ALL)
