set(LIB_SOURCES
  metastore_client.h
  metadata_tree.cpp
  metadata_tree.h
  common.cpp
  common.h
  validation.cpp
  validation.h
)

if(TOOLS_USE_ARROW)
  list(APPEND LIB_SOURCES
    s3client.h
  )
endif()

add_library(iceberg-tools ${LIB_SOURCES})
target_include_directories(iceberg-tools PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(iceberg-tools PUBLIC _hivemetastore
  iceberg-cpp _rapidjson _avrocpp _hivemetastore absl::flags absl::flags_parse Parquet::parquet_static
  OpenSSL::Crypto OpenSSL::SSL)
if (TOOLS_USE_ARROW)
  target_link_libraries(iceberg-tools PUBLIC Arrow::arrow_static)
endif()
if(ICECXX_BUILD_BOOST)
  target_link_libraries(iceberg-tools PUBLIC _boost_iostreams)
else()
  find_package(Boost REQUIRED CONFIG COMPONENTS iostreams)
  target_link_libraries(iceberg-tools PUBLIC Boost::iostreams)
endif()
if (ICECXX_BUILD_THRIFT)
  target_link_libraries(iceberg-tools PUBLIC _thrift)
else()
  find_package(Thrift REQUIRED CONFIG)
  target_link_libraries(iceberg-tools PUBLIC thrift::thrift)
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

add_executable(hive_metastore_client hive_metastore_client.cpp)
target_link_libraries(hive_metastore_client PUBLIC _hivemetastore _thrift)
target_include_directories(hive_metastore_client PUBLIC ${PROJECT_SOURCE_DIR})

add_executable(hive_metastore_server hive_metastore_server.cpp)
target_link_libraries(hive_metastore_server PUBLIC _hivemetastore _thrift)
target_include_directories(hive_metastore_server PUBLIC ${PROJECT_SOURCE_DIR})

if(NOT APPLE)
  target_link_libraries(hive_metastore_server PUBLIC pthread)
endif()

if(TOOLS_USE_ARROW)
  if (ICECXX_STATISTICS)
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
  target_link_libraries(ice_make iceberg-tools iceberg_common_fs ${CMAKE_DL_LIBS})
  set_target_properties(ice_make PROPERTIES LINK_FLAGS_RELEASE -s)
  set_target_properties(ice_make PROPERTIES LINK_FLAGS_RELWITHDEBINFO -s)
endif()
