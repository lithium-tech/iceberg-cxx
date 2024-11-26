add_library(iceberg-cpp STATIC
    catalog.h
    manifest_entry.cpp
    manifest_entry.h
    manifest_file.h
    nested_field.h
    schema.cpp
    schema.h
    snapshot.h
    table_metadata.cpp
    table_metadata.h
    table.h
    tea_column_stats.h
    tea_hive_catalog.cpp
    tea_manifest_metadata.cpp
    tea_scan.cpp
    tea_scan.h
    type.cpp
    type.h
    uuid.cpp
    uuid.h
    write.cpp
    write.h
    transforms.cpp
    transforms.h
    hash_utils.h
    sql_catalog.cpp
    sql_catalog.h
    transaction.cpp
    transaction.h
)

add_subdirectory(parquet)

target_link_libraries(iceberg-cpp PUBLIC rapidjson _avrocpp _boost_iostreams _hivemetastore Parquet::parquet_static ice_parquet)
target_link_libraries(iceberg-cpp PUBLIC Arrow::arrow_static)
if (USE_SMHASHER)
    target_link_libraries(iceberg-cpp PUBLIC SMHasherSupport)
endif()

set_target_properties(iceberg-cpp PROPERTIES POSITION_INDEPENDENT_CODE ON)
