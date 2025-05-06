add_executable(run_performance_tests run_performance_tests.cpp)
target_link_libraries(run_performance_tests PUBLIC iceberg-cpp dl absl::flags absl::flags_parse)

target_compile_options(run_performance_tests PRIVATE -fno-omit-frame-pointer)

ADD_CUSTOM_TARGET(test_performance_warehouse_symlink ALL
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/warehouse" "${CMAKE_CURRENT_BINARY_DIR}/warehouse"
)
add_dependencies(run_performance_tests test_performance_warehouse_symlink)
