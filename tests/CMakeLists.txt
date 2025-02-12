add_executable(iceberg_local_test
    manifest_entry_test.cpp
    manifest_metadata_test.cpp
    validation_test.cpp
    table_metadata_test.cpp
    snapshot_test.cpp
    run_tests.cpp
    compat/transforms_test.cpp
    compat/partitions_test.cpp
)
target_link_libraries(iceberg_local_test PUBLIC gtest iceberg-cpp dl iceberg-tools)
target_include_directories(iceberg_local_test PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")

if (USE_SMHASHER)
    target_link_libraries(iceberg_local_test PUBLIC SMHasherSupport)
endif()

ADD_CUSTOM_TARGET(test_metadata_symlink ALL
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/metadata" "${CMAKE_CURRENT_BINARY_DIR}/metadata"
)
add_dependencies(iceberg_local_test test_metadata_symlink)

ADD_CUSTOM_TARGET(test_data_symlink ALL
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/data" "${CMAKE_CURRENT_BINARY_DIR}/data"
)
add_dependencies(iceberg_local_test test_data_symlink)

ADD_CUSTOM_TARGET(test_snapshots_symlink ALL
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/snapshots" "${CMAKE_CURRENT_BINARY_DIR}/snapshots"
)
add_dependencies(iceberg_local_test test_snapshots_symlink)

ADD_CUSTOM_TARGET(test_warehouse_symlink ALL
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/warehouse" "${CMAKE_CURRENT_BINARY_DIR}/warehouse"
)
add_dependencies(iceberg_local_test test_warehouse_symlink)
