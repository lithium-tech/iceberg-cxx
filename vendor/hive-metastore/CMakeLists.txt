set (CMAKE_CXX_STANDARD 20)

set (SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/hive_metastore_constants.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/hive_metastore_types.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ThriftHiveMetastore.cpp
)

add_library(_hivemetastore ${SRCS})
target_link_libraries(_hivemetastore PUBLIC _thrift)
target_include_directories(_hivemetastore SYSTEM BEFORE PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(_hivemetastore PROPERTIES POSITION_INDEPENDENT_CODE ON)
