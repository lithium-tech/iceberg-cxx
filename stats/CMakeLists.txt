add_library(analyzer
  analyzer.cpp
)

target_link_libraries(analyzer PUBLIC Arrow::arrow_static)

set(DATASKETCHES_DIR ${CMAKE_SOURCE_DIR}/vendor/datasketches-cpp)
target_include_directories(analyzer PUBLIC ${CMAKE_SOURCE_DIR}
  ${DATASKETCHES_DIR}/hll/include
  ${DATASKETCHES_DIR}/quantiles/include
  ${DATASKETCHES_DIR}/fi/include
  ${DATASKETCHES_DIR}/common/include
  ${DATASKETCHES_DIR}/theta/include
)

target_include_directories(analyzer PUBLIC ${CMAKE_SOURCE_DIR})

add_executable(stats_main
  main.cpp
)

target_link_libraries(stats_main Arrow::arrow_static Parquet::parquet_static absl::flags absl::flags_parse analyzer iceberg ${CMAKE_DL_LIBS})
