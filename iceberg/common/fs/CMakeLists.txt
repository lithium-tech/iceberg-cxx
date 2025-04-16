add_library(iceberg_common_fs STATIC
    url.cpp
    s3.cpp
)

target_include_directories(iceberg_common_fs PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(iceberg_common_fs PUBLIC Arrow::arrow_static absl::base)

add_executable(
  iceberg_common_fs_test
  ut/url_test.cpp)

target_link_libraries(iceberg_common_fs_test PRIVATE gtest_main iceberg_common_fs)
target_include_directories(iceberg_common_fs_test PUBLIC ${CMAKE_SOURCE_DIR})
