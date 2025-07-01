if(${ICECXX_COMPUTE})
  add_subdirectory(compute)
  set(ICE_COMPUTE ice_compute)
endif()

add_subdirectory(iceberg)

add_executable(iceberg_streams_ut
  # ut/builder_test.cpp # TODO(gmusya): cover
  ut/data_scan_test.cpp
  ut/equality_delete_applier_test.cpp
  ut/file_reader_builder_test.cpp
  ut/file_reader_test.cpp
  ut/mapper_test.cpp
  ut/positional_delete_applier_test.cpp
  ut/projection_stream_test.cpp
  ut/row_group_reader_test.cpp
  ut/parquet_stats_getter_test.cpp
)

target_link_libraries(iceberg_streams_ut PUBLIC iceberg_streams iceberg_test_utils gtest_main iceberg_common_fs ${CMAKE_DL_LIBS})

if(NOT APPLE)
  target_link_options(iceberg_streams_ut PRIVATE "-Wl,--allow-multiple-definition")
endif()

add_executable(iceberg_streams_cli main.cpp)

target_link_libraries(iceberg_streams_cli PUBLIC iceberg_streams iceberg_common_fs ${CMAKE_DL_LIBS})

if(NOT APPLE)
  target_link_options(iceberg_streams_cli PRIVATE "-Wl,--allow-multiple-definition")
endif()
