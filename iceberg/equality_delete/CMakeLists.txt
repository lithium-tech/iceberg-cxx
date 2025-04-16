add_library(iceberg_equality_delete STATIC
  common.cpp
  common.h
  delete.cpp
  delete.h
  generic_delete.cpp
  generic_delete.h
  handler.cpp
  handler.h
  specialized_one_column_delete.h
  specialized_multiple_column_delete.h
  utils.h
)

set_target_properties(iceberg_equality_delete PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(iceberg_equality_delete PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(iceberg_equality_delete PUBLIC Arrow::arrow_static Parquet::parquet_static absl::base)

add_executable(iceberg_equality_delete_test
  ut/allocator_test.cpp
  ut/equality_delete_test.cpp
  ut/generic_delete_test.cpp
  ut/specialized_delete_test.cpp
)

target_link_libraries(iceberg_equality_delete_test PUBLIC iceberg_equality_delete iceberg_test_utils absl::base gtest_main ${CMAKE_DL_LIBS})
if (NOT APPLE)
  target_link_options(iceberg_equality_delete_test PRIVATE "-Wl,--allow-multiple-definition")
endif()
