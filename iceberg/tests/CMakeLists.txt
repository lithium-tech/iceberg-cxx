add_executable(iceberg_test test.cpp)
target_link_libraries(iceberg_test PRIVATE gtest_main iceberg)

add_test(NAME iceberg_test COMMAND iceberg_test)
add_dependencies(check iceberg_test)
