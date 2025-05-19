add_executable(meta_perf_test run_performance_tests.cpp)
target_link_libraries(meta_perf_test PUBLIC iceberg-cpp dl absl::flags absl::flags_parse)
target_compile_options(meta_perf_test PRIVATE -fno-omit-frame-pointer)

ADD_CUSTOM_TARGET(test_performance_warehouse_symlink ALL
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/warehouse" "${CMAKE_CURRENT_BINARY_DIR}/warehouse"
)
add_dependencies(meta_perf_test test_performance_warehouse_symlink)
