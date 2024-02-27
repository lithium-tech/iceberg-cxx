add_library(iceberg
    field.h
    metadata.cpp
    metadata.h
    schema.h
    snapshot.h
    type.cpp
    type.h
)

target_link_libraries(iceberg PRIVATE rapidjson _avrocpp)
target_include_directories(iceberg PUBLIC "${PROJECT_SOURCE_DIR}")
