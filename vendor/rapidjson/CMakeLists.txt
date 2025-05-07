file(GLOB_RECURSE rapidjson_src "include/rapidjson/*.h")

add_library(_rapidjson INTERFACE ${rapidjson_src})
target_include_directories(_rapidjson INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
