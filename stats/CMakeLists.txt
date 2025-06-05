add_library(analyzer
  analyzer.cpp
  puffin.cpp
)

target_link_libraries(analyzer PUBLIC datasketches Parquet::parquet_static Arrow::arrow_static)

target_include_directories(analyzer PUBLIC ${CMAKE_SOURCE_DIR})

add_executable(stats_main
  main.cpp
)

target_link_libraries(stats_main Arrow::arrow_static Parquet::parquet_static absl::flags absl::flags_parse iceberg analyzer ${CMAKE_DL_LIBS})
