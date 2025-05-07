set(LIB_SOURCES
  metastore_client.h
  metadata_tree.cpp
  metadata_tree.h
  common.cpp
  common.h
  validation.cpp
  validation.h
)

if(USE_ARROW)
  list(APPEND LIB_SOURCES
    s3client.h
  )
endif()

add_library(iceberg-tools ${LIB_SOURCES})
target_include_directories(iceberg-tools PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(iceberg-tools PUBLIC _hivemetastore _thrift
  iceberg-cpp _rapidjson _avrocpp _boost_iostreams _hivemetastore absl::flags absl::flags_parse Parquet::parquet_static
  OpenSSL::Crypto OpenSSL::SSL)
if (USE_ARROW)
  target_link_libraries(iceberg-tools PUBLIC Arrow::arrow_static)
endif()


add_executable(fix_location fix_location.cpp)
target_include_directories(fix_location PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(fix_location iceberg-tools ${CMAKE_DL_LIBS})
set_target_properties(fix_location PROPERTIES LINK_FLAGS_RELEASE -s)
set_target_properties(fix_location PROPERTIES LINK_FLAGS_RELWITHDEBINFO -s)

add_executable(ice_validator validator_tool.cpp)
target_include_directories(ice_validator PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(ice_validator iceberg-tools ${CMAKE_DL_LIBS})
set_target_properties(ice_validator PROPERTIES LINK_FLAGS_RELEASE -s)
set_target_properties(ice_validator PROPERTIES LINK_FLAGS_RELWITHDEBINFO -s)

if(USE_ARROW)
  if (ICEBERG_STATISTICS)
    add_executable(ice_explorer explorer.cpp)
    target_include_directories(ice_explorer PUBLIC "${PROJECT_SOURCE_DIR}")
    target_link_libraries(ice_explorer iceberg-tools analyzer)
  endif()

  add_executable(ice_ls ls.cpp)
  target_include_directories(ice_ls PUBLIC "${PROJECT_SOURCE_DIR}")
  target_link_libraries(ice_ls iceberg-tools ${CMAKE_DL_LIBS})
  set_target_properties(ice_ls PROPERTIES LINK_FLAGS_RELEASE -s)
  set_target_properties(ice_ls PROPERTIES LINK_FLAGS_RELWITHDEBINFO -s)

  add_executable(ice_make make_metadata.cpp)
  target_include_directories(ice_make PUBLIC "${PROJECT_SOURCE_DIR}")
  target_link_libraries(ice_make iceberg-tools ${CMAKE_DL_LIBS})
  set_target_properties(ice_make PROPERTIES LINK_FLAGS_RELEASE -s)
  set_target_properties(ice_make PROPERTIES LINK_FLAGS_RELWITHDEBINFO -s)
endif()
