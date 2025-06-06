add_library(ice_compute STATIC
  projection.cpp
  projection.h
  ssa_program.cpp
  ssa_program.h
)

target_link_libraries(ice_compute PRIVATE Arrow::arrow_static)
target_include_directories(ice_compute PUBLIC ${CMAKE_SOURCE_DIR})
set_target_properties(ice_compute PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_executable(ice_compute_ut
  ut/program_step_test.cpp
)
target_link_libraries(ice_compute_ut ice_compute Arrow::arrow_static gtest_main ${CMAKE_DL_LIBS})
