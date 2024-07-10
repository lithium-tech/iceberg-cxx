add_executable(tpch-gen
  list.cpp
  main.cpp
  text.cpp
)

target_include_directories(tpch-gen PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(tpch-gen data-gen)
