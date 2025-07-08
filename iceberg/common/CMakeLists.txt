add_subdirectory(fs)

add_library(iceberg_common STATIC
    rg_metadata.cpp
    json_parse.cpp
)

target_include_directories(iceberg_common PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(iceberg_common PUBLIC Arrow::arrow_static Parquet::parquet_static)
