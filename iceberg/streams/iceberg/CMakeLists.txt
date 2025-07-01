add_library(iceberg_streams STATIC
  parquet_stats_getter.cpp
)

target_include_directories(iceberg_streams PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(iceberg_streams PUBLIC Arrow::arrow_static Parquet::parquet_static iceberg iceberg_common iceberg_positional_delete iceberg_equality_delete ${ICE_COMPUTE})
