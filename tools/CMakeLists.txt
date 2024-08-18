add_executable(fix_location fix_location.cpp metadata_tree.cpp)
target_include_directories(fix_location PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(fix_location
  iceberg-cpp rapidjson _avrocpp _boost_iostreams _hivemetastore absl::flags absl::flags_parse)

add_executable(ice_ls ls.cpp metadata_tree.cpp)
target_include_directories(ice_ls PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(ice_ls PUBLIC _hivemetastore _thrift
  iceberg-cpp rapidjson _avrocpp _boost_iostreams _hivemetastore absl::flags absl::flags_parse Arrow::arrow_static)
set_target_properties(ice_ls PROPERTIES LINK_FLAGS_RELEASE -s)
set_target_properties(ice_ls PROPERTIES LINK_FLAGS_RELWITHDEBINFO -s)

add_library(iceberg-tools
  common.cpp
  common.h
  metadata_tree.cpp
  metadata_tree.h
  metastore_client.h
  s3client.h
)

target_include_directories(iceberg-tools PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(iceberg-tools PUBLIC _hivemetastore _thrift
  iceberg-cpp rapidjson _avrocpp _boost_iostreams _hivemetastore absl::flags absl::flags_parse Arrow::arrow_static Parquet::parquet_static)
