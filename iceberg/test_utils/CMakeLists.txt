set(test_utils_sources
    arrow_array.cpp
    arrow_array.h
    column.cpp
    column.h
    scoped_temp_dir.cpp
    scoped_temp_dir.h
    write.cpp
    write.h
)

add_library(iceberg_test_utils STATIC ${test_utils_sources})

target_include_directories(
  iceberg_test_utils PUBLIC ${CMAKE_SOURCE_DIR} ${Greenplum_INCLUDE_SERVER})
target_link_libraries(iceberg_test_utils PUBLIC Arrow::arrow_static
                                                   Parquet::parquet_static iceberg)

if (NOT APPLE)
  target_link_options(iceberg_test_utils PRIVATE "-Wl,--allow-multiple-definition")
endif()
