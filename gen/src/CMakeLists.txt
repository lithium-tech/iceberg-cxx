add_library(data-gen
  generators.cpp
)

target_include_directories(data-gen PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(data-gen PUBLIC Arrow::arrow_static Parquet::parquet_static)

if (HAS_ARROW_CSV)
  target_compile_definitions(data-gen PUBLIC HAS_ARROW_CSV)
endif()
