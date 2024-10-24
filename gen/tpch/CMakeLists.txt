add_library(tpch-gen-lib STATIC
  list.cpp
  tables.cpp
  text.cpp
  dataset.cpp
)

target_include_directories(tpch-gen-lib PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(tpch-gen-lib PUBLIC data-gen absl::flags absl::flags_parse)

add_executable(tpch-gen
  main.cpp
)

target_include_directories(tpch-gen PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(tpch-gen tpch-gen-lib absl::flags absl::flags_parse)
