set(LIBRARY_DIR ${CMAKE_CURRENT_SOURCE_DIR})

set(CMAKE_C_STANDARD 17)
set(croaring_SOURCES roaring.hh roaring.c)

add_library(_croaring ${croaring_SOURCES})
target_include_directories(_croaring SYSTEM PUBLIC "${LIBRARY_DIR}")
set_target_properties(_croaring PROPERTIES POSITION_INDEPENDENT_CODE ON)
