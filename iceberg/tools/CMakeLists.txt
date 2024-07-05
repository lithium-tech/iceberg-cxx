add_executable(hive_metastore_client hive_metastore_client.cpp)
target_link_libraries(hive_metastore_client PUBLIC _hivemetastore _thrift)

add_executable(hive_metastore_server hive_metastore_server.cpp)
target_link_libraries(hive_metastore_server PUBLIC _hivemetastore _thrift)

if(NOT APPLE)
  target_link_libraries(hive_metastore_server PUBLIC pthread)
endif()

add_executable(fix_location fix_location.cpp metadata_tree.cpp)
target_include_directories(fix_location PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(fix_location
  iceberg rapidjson _avrocpp _boost_iostreams _hivemetastore absl::flags absl::flags_parse)

if(USE_ICEBERG AND ${pg_version_major} EQUAL 9)
  add_executable(syncice syncice.cpp copy_modified.cpp metadata_tree.cpp)
  target_include_directories(syncice PUBLIC "${PROJECT_SOURCE_DIR} ${ArrowHouse_SOURCE_DIR}")
  target_link_libraries(syncice PUBLIC _hivemetastore _thrift
    iceberg rapidjson _avrocpp _boost_iostreams _hivemetastore absl::flags absl::flags_parse
    arrowhouse DataStreams Arrow::arrow_static Parquet::parquet_static)
  set_target_properties(syncice PROPERTIES LINK_FLAGS_RELEASE -s)
  set_target_properties(syncice PROPERTIES LINK_FLAGS_RELWITHDEBINFO -s)

  add_executable(ice_ls ls.cpp metadata_tree.cpp)
  target_include_directories(ice_ls PUBLIC "${PROJECT_SOURCE_DIR}")
  target_link_libraries(ice_ls PUBLIC _hivemetastore _thrift
    iceberg rapidjson _avrocpp _boost_iostreams _hivemetastore absl::flags absl::flags_parse Arrow::arrow_static)
  set_target_properties(ice_ls PROPERTIES LINK_FLAGS_RELEASE -s)
  set_target_properties(ice_ls PROPERTIES LINK_FLAGS_RELWITHDEBINFO -s)

  install(TARGETS syncice ice_ls hive_metastore_client hive_metastore_server
    DESTINATION ""
    COMPONENT icetools
    EXCLUDE_FROM_ALL
  )
endif()
