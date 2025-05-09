set (BOOST_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

# headers-only

add_library (_boost_headers_only INTERFACE)
add_library (boost::headers_only ALIAS _boost_headers_only)
target_include_directories (_boost_headers_only SYSTEM BEFORE INTERFACE ${BOOST_ROOT_DIR})

# iostreams

set (IOSTREAMS_PATH ${BOOST_ROOT_DIR}/libs/iostreams)

set (SRCS_IOSTREAMS
  "${IOSTREAMS_PATH}/src/file_descriptor.cpp"
  "${IOSTREAMS_PATH}/src/gzip.cpp"
  "${IOSTREAMS_PATH}/src/mapped_file.cpp"
  "${IOSTREAMS_PATH}/src/zlib.cpp"
  "${IOSTREAMS_PATH}/src/zstd.cpp"
)

add_library (_boost_iostreams ${SRCS_IOSTREAMS})
add_library (boost::iostreams ALIAS _boost_iostreams)
target_include_directories (_boost_iostreams PRIVATE ${BOOST_ROOT_DIR} ${IOSTREAMS_PATH}/include)
target_link_libraries (_boost_iostreams PRIVATE _zlib)
target_link_libraries (_boost_iostreams PRIVATE _zstd)
set_target_properties(_boost_iostreams PROPERTIES POSITION_INDEPENDENT_CODE ON)
