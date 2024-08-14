add_executable(iceberg_local_test
    manifest_entry_test.cpp
    manifest_metadata_test.cpp
    table_metadata_test.cpp
    run_tests.cpp
)
target_link_libraries(iceberg_local_test PRIVATE gtest iceberg-cpp dl)
target_include_directories(iceberg_local_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

add_executable(iceberg_test
    hive_client_test.cpp
    scan_test.cpp
    run_tests.cpp
)
target_link_libraries(iceberg_test PRIVATE gtest iceberg-cpp dl)
target_include_directories(iceberg_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

ADD_CUSTOM_TARGET(test_data_symlink ALL
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/metadata" "${CMAKE_CURRENT_BINARY_DIR}/metadata"
)
add_dependencies(iceberg_local_test test_data_symlink)
add_dependencies(iceberg_test test_data_symlink)
