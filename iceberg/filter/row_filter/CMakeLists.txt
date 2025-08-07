add_library(row_filter STATIC
  arrow_types.cpp
  arrow_types.h
  registry.cpp
  registry.h
  row_filter.cpp
  row_filter.h
  extend_gandiva/arithmetic_ops.h
  extend_gandiva/epoch_time_point.cpp
  extend_gandiva/epoch_time_point.h
  extend_gandiva/function_registry_arithmetic.cpp
  extend_gandiva/function_registry_arithmetic.h
  extend_gandiva/function_registry_temporal.cpp
  extend_gandiva/function_registry_temporal.h
  extend_gandiva/register_functions.cpp
  extend_gandiva/register_functions.h
  extend_gandiva/temporal_ops.cpp
  extend_gandiva/temporal_ops.h
)

target_link_libraries(row_filter PUBLIC iceberg filter_representation Arrow::arrow_static Gandiva::gandiva_static)
target_include_directories(row_filter PUBLIC ${CMAKE_SOURCE_DIR})

set_target_properties(row_filter PROPERTIES POSITION_INDEPENDENT_CODE ON)


add_executable(row_filter_ut
  extend_gandiva/ut/arithmetic_ops_test.cpp
  extend_gandiva/ut/temporal_ops_test.cpp
  ut/mock.cpp
  ut/row_filter_test.cpp
)
target_link_libraries(row_filter_ut gtest_main gmock iceberg_test_utils row_filter Arrow::arrow_static Gandiva::gandiva_static)
