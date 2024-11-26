add_library(ice_parquet STATIC
    io.cpp
    io.h
)

target_link_libraries(ice_parquet PUBLIC rapidjson _avrocpp _boost_iostreams _hivemetastore Parquet::parquet_static)
target_link_libraries(ice_parquet PUBLIC Arrow::arrow_static)

if (USE_SMHASHER)
    target_link_libraries(ice_parquet PUBLIC SMHasherSupport)
endif()

set_target_properties(ice_parquet PROPERTIES POSITION_INDEPENDENT_CODE ON)
