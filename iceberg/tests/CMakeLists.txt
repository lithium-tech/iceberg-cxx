add_subdirectory(src)

if(ICEBERG_LOCAL_TESTS)
    add_compile_definitions(ICEBERG_LOCAL_TESTS)
endif()

add_executable(iceberg_test
    hive_client_test.cpp
    manifest_entry_test.cpp
    manifest_metadata_test.cpp
    scan_test.cpp
    table_metadata_test.cpp
)
target_link_libraries(iceberg_test PRIVATE gtest_main iceberg iceberg_test_utils)

add_test(NAME iceberg_test COMMAND iceberg_test)
add_dependencies(check iceberg_test)

ADD_CUSTOM_TARGET(test_data_symlink ALL
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/data" "${CMAKE_CURRENT_BINARY_DIR}/data"
)
add_dependencies(iceberg_test test_data_symlink)
