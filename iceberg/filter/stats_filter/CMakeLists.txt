add_library(stats_filter STATIC
  registry.cpp
  registry.h
  stats_filter.cpp
  stats_filter.h
  stats.h
)

target_link_libraries(stats_filter PRIVATE filter_representation Arrow::arrow_static)
target_include_directories(stats_filter PUBLIC ${CMAKE_SOURCE_DIR})

set_target_properties(stats_filter PROPERTIES POSITION_INDEPENDENT_CODE ON)


add_executable(stats_filter_ut
  ut/stats_filter_test.cpp
)
target_link_libraries(stats_filter_ut iceberg_test_utils stats_filter Arrow::arrow_static gtest_main ${CMAKE_DL_LIBS})
