add_library(filter_representation STATIC
  function.h
  node.h
  serializer.cpp
  serializer.h
  tree_rewriter.cpp
  tree_rewriter.h
  value.cpp
  value.h
  visitor.h
)

target_link_libraries(filter_representation PUBLIC _rapidjson yyjson)
target_link_libraries(filter_representation PRIVATE Arrow::arrow_static)
target_include_directories(filter_representation PUBLIC ${CMAKE_SOURCE_DIR})

set_target_properties(filter_representation PROPERTIES POSITION_INDEPENDENT_CODE ON)


add_executable(filter_representation_ut
  ut/node_test.cpp
  ut/serializer_test.cpp
  ut/tree_rewriter_test.cpp
  ut/visitor_test.cpp
)
target_link_libraries(filter_representation_ut filter_representation Arrow::arrow_static gtest_main)
