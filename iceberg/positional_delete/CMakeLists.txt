add_library(iceberg_positional_delete STATIC
  positional_delete.cpp
  positional_delete.h
)

set_target_properties(iceberg_positional_delete PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(iceberg_positional_delete PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(iceberg_positional_delete PUBLIC Arrow::arrow_static Parquet::parquet_static absl::base)

add_executable(iceberg_positional_delete_test
  ut/positional_delete_test.cpp
)

target_link_libraries(iceberg_positional_delete_test PUBLIC iceberg_positional_delete iceberg_test_utils absl::base gtest_main ${CMAKE_DL_LIBS})
if (NOT APPLE)
  target_link_options(iceberg_positional_delete_test PRIVATE "-Wl,--allow-multiple-definition")
endif()
