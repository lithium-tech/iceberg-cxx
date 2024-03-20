add_executable(hive_metastore_client hive_metastore_client.cpp)
target_link_libraries(hive_metastore_client PUBLIC _hivemetastore _thrift)

add_executable(hive_metastore_server hive_metastore_server.cpp)
target_link_libraries(hive_metastore_server PUBLIC _hivemetastore _thrift)

if(NOT APPLE)
    target_link_libraries(hive_metastore_server PUBLIC pthread)
endif()
