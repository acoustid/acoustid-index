cmake_minimum_required(VERSION 3.1)

project(fpserver)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules)

set(CMAKE_INCLUDE_CURRENT_DIR ON)

set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTOUIC ON)
set(CMAKE_AUTORCC ON)

find_package(GTest)
find_package(Qt5 COMPONENTS Core Network REQUIRED)

set(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_NAME acoustid-index)
set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Lukas Lalinsky <lukas@acoustid.org>")
set(CPACK_DEBIAN_FILE_NAME "${CPACK_PACKAGE_NAME}.deb")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS TRUE)
set(CPACK_STRIP_FILES TRUE)
set(CPACK_COMPONENTS_ALL_IN_ONE_PACKAGE ON)
set(CPACK_COMPONENTS_ALL application)

include(CPack)

add_definitions(
        -D__STDC_LIMIT_MACROS
        -D__STDC_CONSTANT_MACROS
)

set(fpindexlib_SOURCES
	src/index/index.cpp
	src/index/index_file_deleter.cpp
	src/index/index_info.cpp
	src/index/index_reader.cpp
	src/index/index_writer.cpp
	src/index/segment_data_reader.cpp
	src/index/segment_data_writer.cpp
	src/index/segment_index.cpp
	src/index/segment_index_reader.cpp
	src/index/segment_index_writer.cpp
	src/index/segment_info.cpp
	src/index/segment_merge_policy.cpp
	src/index/segment_merger.cpp
	src/index/segment_searcher.cpp
	src/index/top_hits_collector.cpp
	src/store/buffered_input_stream.cpp
	src/store/buffered_output_stream.cpp
	src/store/checksum_output_stream.cpp
	src/store/checksum_input_stream.cpp
	src/store/directory.cpp
	src/store/fs_directory.cpp
	src/store/fs_input_stream.cpp
	src/store/fs_output_stream.cpp
	src/store/input_stream.cpp
	src/store/memory_input_stream.cpp
	src/store/mmap_input_stream.cpp
	src/store/output_stream.cpp
	src/store/ram_directory.cpp
	src/store/ram_output_stream.cpp
	src/util/crc.c
	src/util/options.cpp
)
add_library(fpindexlib ${fpindexlib_SOURCES})
target_link_libraries(fpindexlib Qt5::Core Qt5::Network)

set(fpserver_HEADERS
	src/server/listener.h
	src/server/connection.h
	src/server/handler.h
	src/server/metrics.h
	src/server/metrics_server.h
)
set(fpserver_SOURCES
	src/server/listener.cpp
	src/server/connection.cpp
	src/server/handler.cpp
	src/server/metrics.cpp
	src/server/metrics_server.cpp
	src/server/main.cpp
)

qt5_wrap_cpp(fpserver_MOC ${fpserver_HEADERS})

include_directories(
	${CMAKE_CURRENT_SOURCE_DIR}/src
    ${GTEST_INCLUDE_DIRS}
)

add_executable(fpi-server ${fpserver_SOURCES} ${fpserver_MOC})
target_link_libraries(fpi-server fpindexlib)

add_executable(fpi-add src/tools/fpi-add.cpp)
target_link_libraries(fpi-add fpindexlib)

add_executable(fpi-import src/tools/fpi-import.cpp)
target_link_libraries(fpi-import fpindexlib)

add_executable(fpi-search src/tools/fpi-search.cpp)
target_link_libraries(fpi-search fpindexlib)

#add_executable(fpi-stats src/tools/fpi-stats.cpp)
#target_link_libraries(fpi-stats ${QT_LIBRARIES} fpindexlib)

set(tests_SOURCES
	src/index/segment_data_writer_test.cpp
	src/index/segment_index_test.cpp
	src/index/segment_index_reader_test.cpp
	src/index/segment_index_writer_test.cpp
	src/index/index_test.cpp
	src/index/index_info_test.cpp
	src/index/index_reader_test.cpp
	src/index/index_writer_test.cpp
	src/index/index_file_deleter_test.cpp
	src/index/segment_enum_test.cpp
	src/index/segment_merger_test.cpp
	src/index/segment_merge_policy_test.cpp
	src/index/top_hits_collector_test.cpp
	src/store/buffered_input_stream_test.cpp
	src/store/input_stream_test.cpp
	src/store/output_stream_test.cpp
	src/store/fs_output_stream_test.cpp
	src/store/ram_directory_test.cpp
	src/util/search_utils_test.cpp
	src/util/options_test.cpp
	src/util/exceptions_test.cpp
	src/util/tests.cpp
)

install(
	TARGETS
		fpi-add
		fpi-import
		fpi-search
		fpi-server
	RUNTIME DESTINATION bin
    COMPONENT application
)

if(GTEST_FOUND)
	include_directories(${GTEST_INCLUDE_DIRS})
	add_subdirectory(${GTEST_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/gtest_build)
	add_executable(tests ${tests_SOURCES})
	target_link_libraries(tests
		${GTEST_BOTH_LIBRARIES} -lpthread
		${QT_LIBRARIES}
		fpindexlib)
	enable_testing(true)
	add_test(IndexTests tests)
	add_custom_target(check ${CMAKE_CURRENT_BINARY_DIR}/tests $ENV{GTEST_FLAGS} DEPENDS tests)
endif()
