set(iceberg_cpp_files
  catalog.h
  manifest_entry.cpp
  manifest_entry.h
  manifest_file.h
  nested_field.h
  puffin.cpp
  puffin.h
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
  parquet/io.cpp
  parquet/io.h
  tea_remote_catalog.cpp
  tea_remote_catalog.h
)

if (USE_REST)
  list(APPEND iceberg_cpp_files tea_rest_catalog.cpp)
endif()

add_library(iceberg-cpp STATIC ${iceberg_cpp_files})

target_link_libraries(iceberg-cpp PUBLIC rapidjson _avrocpp _boost_iostreams _hivemetastore Parquet::parquet_static Arrow::arrow_static)
if (USE_REST)
  target_link_libraries(iceberg-cpp PUBLIC cpr)
endif()
if (USE_SMHASHER)
    target_link_libraries(iceberg-cpp PUBLIC SMHasherMurmurHash3)
endif()

set_target_properties(iceberg-cpp PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_executable(iceberg-cpp-test
  ut/puffin_test.cpp
)
target_link_libraries(iceberg-cpp-test iceberg-cpp gtest_main)
