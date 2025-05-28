set(iceberg_local_test_files
    deletion_vector_test.cpp
    manifest_entry_test.cpp
    manifest_metadata_test.cpp
    validation_test.cpp
    table_metadata_test.cpp
    tea_scan_test.cpp
    snapshot_test.cpp
    referenced_data_file_test.cpp
    run_tests.cpp
    compat/partitions_test.cpp
    rest/test_rest_catalog.cpp
)

if (ICECXX_WRITE)
    list(APPEND iceberg_local_test_files compat/transforms_test.cpp)
endif()

if (ICECXX_USE_REST)
    list(APPEND iceberg_local_test_files rest/test_rest_catalog.cpp)
endif()

add_executable(iceberg_local_test ${iceberg_local_test_files})
target_link_libraries(iceberg_local_test PUBLIC gtest iceberg-cpp iceberg-tools ${CMAKE_DL_LIBS})
if (ICECXX_USE_REST)
    target_link_libraries(iceberg_local_test PUBLIC cpr)
endif()
target_include_directories(iceberg_local_test PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")

if (ICECXX_USE_SMHASHER)
    target_link_libraries(iceberg_local_test PUBLIC SMHasherMurmurHash3)
endif()

# TODO(gmusya): get rid of following commands
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

ADD_CUSTOM_TARGET(test_tables_symlink ALL
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/tables" "${CMAKE_CURRENT_BINARY_DIR}/tables"
)
add_dependencies(iceberg_local_test test_tables_symlink)
