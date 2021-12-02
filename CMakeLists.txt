cmake_minimum_required(VERSION 3.1)

project(fpserver)

cmake_policy(SET CMP0071 NEW)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules)

set(CMAKE_INCLUDE_CURRENT_DIR ON)

set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTOUIC ON)
set(CMAKE_AUTORCC ON)

find_package(Threads REQUIRED)
find_package(GTest)
find_package(Qt5 COMPONENTS Core Network Concurrent REQUIRED)

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
    -DQHTTP_HAS_CLIENT
)

set(fpindexlib_SOURCES
    src/index/search_result.h
	src/index/index.cpp
	src/index/index_file_deleter.cpp
	src/index/index_info.cpp
	src/index/index_reader.cpp
	src/index/index_writer.cpp
	src/index/segment_docs.h
	src/index/segment_docs.cpp
	src/index/segment_docs_reader.h
	src/index/segment_docs_reader.cpp
	src/index/segment_docs_writer.h
	src/index/segment_docs_writer.cpp
	src/index/segment_data_reader.cpp
	src/index/segment_data_writer.cpp
	src/index/segment_index.cpp
	src/index/segment_index_reader.cpp
	src/index/segment_index_writer.cpp
	src/index/segment_info.cpp
	src/index/segment_merge_policy.cpp
	src/index/segment_merger.cpp
	src/index/segment_searcher.cpp
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
target_link_libraries(fpindexlib Qt5::Core Qt5::Network Qt5::Concurrent)

set(fpserver_SOURCES
	src/server/listener.cpp
	src/server/protocol.cpp
	src/server/session.cpp
	src/server/connection.cpp
	src/server/metrics.cpp
	src/server/request.cpp
	src/server/http.cpp
)
add_library(fpserverlib ${fpserver_SOURCES})
target_link_libraries(fpserverlib fpindexlib)

set(qhttp_SOURCES
    ./src/3rdparty/qhttp/src/qhttpserverconnection.cpp
    ./src/3rdparty/qhttp/src/qhttpabstracts.cpp
    ./src/3rdparty/qhttp/src/qhttpclientrequest.cpp
    ./src/3rdparty/qhttp/src/qhttpserverresponse.cpp
    ./src/3rdparty/qhttp/src/qhttpserverrequest.cpp
    ./src/3rdparty/qhttp/src/qhttpclient.cpp
    ./src/3rdparty/qhttp/src/qhttpserver.cpp
    ./src/3rdparty/qhttp/src/qhttpclientresponse.cpp
    ./src/3rdparty/qhttp/3rdparty/http-parser/http_parser.c
)
add_library(qhttp ${qhttp_SOURCES})
target_link_libraries(qhttp Qt5::Core Qt5::Network Qt5::Concurrent)

include_directories(
	${CMAKE_CURRENT_SOURCE_DIR}/src
	${CMAKE_CURRENT_SOURCE_DIR}/src/3rdparty/qhttp/3rdparty
	${CMAKE_CURRENT_SOURCE_DIR}/src/3rdparty/qhttp/src
    ${GTEST_INCLUDE_DIRS}
)

add_executable(fpi-server src/server/main.cpp)
target_link_libraries(fpi-server fpserverlib qhttp)

add_executable(fpi-add src/tools/fpi-add.cpp)
target_link_libraries(fpi-add fpindexlib)

add_executable(fpi-import src/tools/fpi-import.cpp)
target_link_libraries(fpi-import fpindexlib)

add_executable(fpi-search src/tools/fpi-search.cpp)
target_link_libraries(fpi-search fpindexlib)

#add_executable(fpi-stats src/tools/fpi-stats.cpp)
#target_link_libraries(fpi-stats ${QT_LIBRARIES} fpindexlib)

set(tests_SOURCES
	src/index/search_result_test.cpp
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
	src/store/buffered_input_stream_test.cpp
	src/store/input_stream_test.cpp
	src/store/output_stream_test.cpp
	src/store/fs_output_stream_test.cpp
	src/store/ram_directory_test.cpp
	src/util/search_utils_test.cpp
	src/util/options_test.cpp
	src/util/exceptions_test.cpp
	src/util/tests.cpp
	src/server/session_test.cpp
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
    add_library(gtest STATIC ${GTEST_SOURCES})
    target_include_directories(gtest INTERFACE ${GTEST_INCLUDE_DIRS})
    target_include_directories(gtest PRIVATE ${GTEST_SOURCE_DIR})
    target_link_libraries(gtest Threads::Threads)

    add_library(gtest_main STATIC ${GTEST_MAIN_SOURCES})
    target_link_libraries(gtest_main gtest)

    add_executable(tests ${tests_SOURCES})
    target_link_libraries(tests gtest gtest_main fpindexlib fpserverlib)

    enable_testing(true)
    add_test(IndexTests tests)

    add_custom_target(check ${CMAKE_CURRENT_BINARY_DIR}/tests $ENV{GTEST_FLAGS} DEPENDS tests)
endif()
