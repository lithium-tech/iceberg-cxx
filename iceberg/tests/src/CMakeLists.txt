add_library(iceberg_test_utils
    config.cpp
    config.h
)

target_include_directories(iceberg_test_utils PUBLIC "${PROJECT_SOURCE_DIR}")
target_link_libraries(iceberg_test_utils PRIVATE Arrow::arrow_static ini)
