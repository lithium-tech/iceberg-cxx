add_subdirectory(fs)

add_library(iceberg_common STATIC
    rg_metadata.cpp
    json_parse.cpp
)

target_include_directories(iceberg_common PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(iceberg_common PUBLIC Arrow::arrow_static Parquet::parquet_static _rapidjson)

add_executable(iceberg_cxx_common_test
  ut/threadpool_test.cpp
)
target_link_libraries(iceberg_cxx_common_test iceberg_common gtest_main)
