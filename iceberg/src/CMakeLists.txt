add_library(iceberg
    metadata.cpp
    metadata.h
)

target_link_libraries(iceberg PRIVATE rapidjson _avrocpp)
target_include_directories(iceberg PUBLIC "${PROJECT_SOURCE_DIR}")
