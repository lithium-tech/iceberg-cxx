add_library(data-gen
  generators.cpp
)

target_include_directories(data-gen PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(data-gen PUBLIC Arrow::arrow_static Parquet::parquet_static)
