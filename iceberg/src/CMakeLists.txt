add_library(iceberg
    field.h
    hive_client.cpp
    hive_client.h
    manifest_entry.cpp
    manifest_entry.h
    manifest_metadata.cpp
    manifest_metadata.h
    scan.cpp
    scan.h
    schema.h
    snapshot.h
    table_metadata.cpp
    table_metadata.h
    type.cpp
    type.h
)

target_link_libraries(iceberg PRIVATE rapidjson _avrocpp _boost_iostreams _hivemetastore)
target_link_libraries(iceberg PUBLIC Arrow::arrow_static)
target_include_directories(iceberg PUBLIC "${PROJECT_SOURCE_DIR}" "${PROJECT_SOURCE_DIR}/vendor/boost")
set_target_properties(iceberg PROPERTIES POSITION_INDEPENDENT_CODE ON)
