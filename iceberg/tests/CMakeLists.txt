add_executable(iceberg_test
    hive_client_test.cpp
    manifest_entry_test.cpp
    manifest_metadata_test.cpp
    scan_test.cpp
    table_metadata_test.cpp
    run_tests.cpp
)
target_link_libraries(iceberg_test PRIVATE gtest iceberg dl)

ADD_CUSTOM_TARGET(test_data_symlink ALL
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/metadata" "${CMAKE_CURRENT_BINARY_DIR}/metadata"
)
add_dependencies(iceberg_test test_data_symlink)
