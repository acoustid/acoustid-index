cmake_minimum_required(VERSION 2.6)

project(fpserver)

find_package(GTest)
find_package(Qt4 COMPONENTS QtCore QtNetwork REQUIRED)
include(${QT_USE_FILE})

add_definitions(
        -D__STDC_LIMIT_MACROS
        -D__STDC_CONSTANT_MACROS
)

set(fpindexlib_SOURCES
	src/index/segment_data_reader.cpp
	src/index/segment_data_writer.cpp
	src/index/segment_index.cpp
	src/index/segment_index_reader.cpp
	src/index/segment_index_writer.cpp
	src/index/index_info.cpp
	src/index/segment_searcher.cpp
	src/index/segment_merger.cpp
	src/index/segment_merge_policy.cpp
	src/index/top_hits_collector.cpp
	src/index/index.cpp
	src/index/index_writer.cpp
	src/index/index_reader.cpp
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
	src/util/options.cpp
)
add_library(fpindexlib ${fpindexlib_SOURCES})

set(fpserver_HEADERS
	src/server/listener.h
	src/server/connection.h
	src/server/handler.h
)
set(fpserver_SOURCES
	src/server/listener.cpp
	src/server/connection.cpp
	src/server/handler.cpp
	src/server/main.cpp
)

qt4_wrap_cpp(fpserver_MOC ${fpserver_HEADERS})

include_directories(
	${CMAKE_CURRENT_BINARY_DIR}
	${CMAKE_CURRENT_SOURCE_DIR}/src
    ${GTEST_INCLUDE_DIRS}
)

add_executable(fpserver ${fpserver_SOURCES} ${fpserver_MOC})
target_link_libraries(fpserver ${QT_LIBRARIES} fpindexlib)

#add_executable(fpindex src/tools/fpindex.cpp)
#target_link_libraries(fpindex ${QT_LIBRARIES} fpindexlib)

#add_executable(fpstats src/tools/fpstats.cpp)
#target_link_libraries(fpstats ${QT_LIBRARIES} fpindexlib)

#add_executable(fpsearch src/tools/fpsearch.cpp)
#target_link_libraries(fpsearch ${QT_LIBRARIES} fpindexlib)

#add_executable(fpdumpindex src/tools/fpdumpindex.cpp)
#target_link_libraries(fpdumpindex ${QT_LIBRARIES} fpindexlib)

add_executable(fpi-add src/tools/fpi-add.cpp)
target_link_libraries(fpi-add ${QT_LIBRARIES} fpindexlib)

add_executable(fpi-import src/tools/fpi-import.cpp)
target_link_libraries(fpi-import ${QT_LIBRARIES} fpindexlib)

add_executable(fpi-search src/tools/fpi-search.cpp)
target_link_libraries(fpi-search ${QT_LIBRARIES} fpindexlib)

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
	src/index/segment_enum_test.cpp
	src/index/segment_merger_test.cpp
	src/index/segment_merge_policy_test.cpp
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

if(GTEST_FOUND)
	add_executable(tests ${tests_SOURCES})
	target_link_libraries(tests
		${GTEST_BOTH_LIBRARIES} -lpthread
		${QT_LIBRARIES}
		fpindexlib)
	add_custom_target(check ./tests $ENV{GTEST_FLAGS} DEPENDS tests)
endif()

