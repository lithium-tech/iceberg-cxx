add_executable(hive_metastore_client hive_metastore_client.cpp)
target_link_libraries(hive_metastore_client PUBLIC _hivemetastore _thrift)

add_executable(hive_metastore_server hive_metastore_server.cpp)
target_link_libraries(hive_metastore_server PUBLIC _hivemetastore _thrift)

if(NOT APPLE)
    target_link_libraries(hive_metastore_server PUBLIC pthread)
endif()

add_executable(fix_location fix_location.cpp)
target_include_directories(fix_location PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(fix_location
    iceberg rapidjson _avrocpp _boost_iostreams _hivemetastore absl::flags absl::flags_parse)
