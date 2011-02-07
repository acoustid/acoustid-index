cmake_minimum_required(VERSION 2.6)

project(fpserver)

find_package(GTest)
find_package(Qt4 COMPONENTS QtCore QtNetwork REQUIRED)
include(${QT_USE_FILE})

set(fpindexlib_SOURCES
	src/index/segment_data_reader.cpp
	src/index/segment_data_writer.cpp
	src/index/segment_index.cpp
	src/index/segment_index_reader.cpp
	src/index/segment_index_writer.cpp
	src/index/segment_info_list.cpp
	src/index/segment_searcher.cpp
	src/index/top_hits_collector.cpp
	src/index/index_writer.cpp
	src/store/buffered_input_stream.cpp
	src/store/buffered_output_stream.cpp
	src/store/input_stream.cpp
	src/store/output_stream.cpp
	src/store/directory.cpp
	src/store/fs_directory.cpp
	src/store/fs_input_stream.cpp
	src/store/fs_output_stream.cpp
	src/store/mmap_input_stream.cpp
	src/store/memory_input_stream.cpp
	src/store/ram_directory.cpp
	src/store/ram_output_stream.cpp
)
add_library(fpindexlib ${fpindexlib_SOURCES})

set(fpserver_HEADERS
	src/server/listener.h
	src/server/connection.h
)
set(fpserver_SOURCES
	src/server/listener.cpp
	src/server/connection.cpp
	src/server/main.cpp
)

qt4_wrap_cpp(fpserver_MOC ${fpserver_HEADERS})

include_directories(
	${CMAKE_CURRENT_BINARY_DIR}
	${CMAKE_CURRENT_SOURCE_DIR}/src
    ${GTEST_INCLUDE_DIRS}
)

add_executable(fpserver
    ${fpserver_SOURCES}
    ${fpserver_MOC}
)
target_link_libraries(fpserver ${QT_LIBRARIES})

add_executable(fpindex fpindex.cpp)
target_link_libraries(fpindex ${QT_LIBRARIES} fpindexlib)

add_executable(fpstats fpstats.cpp)
target_link_libraries(fpstats ${QT_LIBRARIES} fpindexlib)

add_executable(fpsearch fpsearch.cpp)
target_link_libraries(fpsearch ${QT_LIBRARIES} fpindexlib)

add_executable(fpsearch2 fpsearch2.cpp)
target_link_libraries(fpsearch2 ${QT_LIBRARIES} fpindexlib)

add_executable(fpdumpindex fpdumpindex.cpp)
target_link_libraries(fpdumpindex ${QT_LIBRARIES} fpindexlib)

set(tests_SOURCES
	src/index/segment_data_writer_test.cpp
	src/index/segment_index_test.cpp
	src/index/segment_index_reader_test.cpp
	src/index/segment_index_writer_test.cpp
	src/store/input_stream_test.cpp
	src/store/output_stream_test.cpp
	src/store/fs_output_stream_test.cpp
	src/store/ram_directory_test.cpp
	src/util/search_utils_test.cpp
	src/util/tests.cpp
)

if(GTEST_FOUND)
	add_executable(tests ${tests_SOURCES})
	target_link_libraries(tests
		${GTEST_BOTH_LIBRARIES} -lpthread
		${QT_LIBRARIES}
		fpindexlib)
	add_custom_target(check ./tests $ENV{GTEST_FLAGS} DEPENDS tests)
endif()

