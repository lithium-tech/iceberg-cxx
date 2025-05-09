add_library(analyzer
  analyzer.cpp
  puffin.cpp
)

target_link_libraries(analyzer PUBLIC iceberg Arrow::arrow_static Parquet::parquet_static)

add_executable(analyzer_test
  ut/analyzer_test.cpp
  ut/sketch_test.cpp
)

target_link_libraries(analyzer_test analyzer iceberg_test_utils gtest_main ${CMAKE_DL_LIBS})

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

target_link_libraries(stats_main Arrow::arrow_static Parquet::parquet_static absl::flags absl::flags_parse analyzer ${CMAKE_DL_LIBS})
