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
find_package(SQLite3 REQUIRED)
find_package(Qt6 COMPONENTS Core Network Concurrent REQUIRED)

find_package(PkgConfig REQUIRED)
pkg_search_module(PROTOBUF REQUIRED IMPORTED_TARGET protobuf)
pkg_search_module(GRPCPP REQUIRED IMPORTED_TARGET grpc++)

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

if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
    add_compile_options(-Wall -pedantic)
endif()

add_definitions(
    -D__STDC_LIMIT_MACROS
    -D__STDC_CONSTANT_MACROS
    -DQHTTP_HAS_CLIENT
)

set(fpindexlib_SOURCES
    src/index/search_result.h
    src/index/base_index.h
	src/index/index.cpp
	src/index/index_file_deleter.cpp
	src/index/index_info.cpp
	src/index/multi_index.h
	src/index/multi_index.cpp
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
	src/index/op.h
	src/index/op.cpp
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
	src/store/sqlite/database.cpp
	src/store/sqlite/database.h
	src/store/sqlite/error.cpp
	src/store/sqlite/error.h
	src/store/sqlite/statement.cpp
	src/store/sqlite/statement.h
	src/util/crc.c
	src/util/options.cpp
	src/util/tracing.cpp
	src/fpindex/search_result.h
	src/fpindex/base_segment.h
	src/fpindex/base_segment.cpp
	src/fpindex/segment_file_format.h
	src/fpindex/segment.h
	src/fpindex/segment.cpp
	src/fpindex/in_memory_segment.h
	src/fpindex/in_memory_segment.cpp
	src/fpindex/proto/internal.pb.h
	src/fpindex/proto/internal.pb.cc
)

add_library(fpindexlib ${fpindexlib_SOURCES})
target_link_libraries(fpindexlib Qt6::Core Qt6::Network Qt6::Concurrent SQLite::SQLite3 PkgConfig::PROTOBUF)

add_custom_command(
    COMMAND
        protoc
          -Isrc/fpindex/proto
	  --cpp_out=src/fpindex/proto
	  src/fpindex/proto/internal.proto
    OUTPUT
      src/fpindex/proto/internal.pb.h
      src/fpindex/proto/internal.pb.cc
    DEPENDS
      src/fpindex/proto/internal.proto
)

set(qhttp_SOURCES
    ./src/3rdparty/qhttp/src/qhttpserverconnection.cpp
    ./src/3rdparty/qhttp/src/qhttpabstracts.cpp
    ./src/3rdparty/qhttp/src/qhttpclientrequest.cpp
    ./src/3rdparty/qhttp/src/qhttpserverresponse.cpp
    ./src/3rdparty/qhttp/src/qhttpserverrequest.cpp
    ./src/3rdparty/qhttp/src/qhttpclient.cpp
    ./src/3rdparty/qhttp/src/qhttpserver.cpp
    ./src/3rdparty/qhttp/src/qhttpclientresponse.cpp
    ./src/3rdparty/http-parser/http_parser.c
)
add_library(qhttp ${qhttp_SOURCES})
target_include_directories(qhttp PRIVATE ./src/3rdparty/)
target_link_libraries(qhttp Qt6::Core Qt6::Network Qt6::Concurrent)

set(fpserver_SOURCES
    src/server/listener.cpp
    src/server/protocol.cpp
    src/server/session.cpp
    src/server/connection.cpp
    src/server/metrics.cpp
    src/server/request.cpp
    src/server/http.cpp
    src/server/http/request.h
    src/server/http/request.cpp
    src/server/http/response.h
    src/server/http/response.cpp
    src/server/http/router.h
    src/server/http/router.cpp
    src/server/grpc/service.h
    src/server/grpc/service.cpp
    src/server/grpc/proto/index.pb.h
    src/server/grpc/proto/index.pb.cc
    src/server/grpc/proto/index.grpc.pb.h
    src/server/grpc/proto/index.grpc.pb.cc
    src/server/grpc/proto/google/api/annotations.pb.h
    src/server/grpc/proto/google/api/annotations.pb.cc
    src/server/grpc/proto/google/api/http.pb.h
    src/server/grpc/proto/google/api/http.pb.cc
)
add_library(fpserverlib ${fpserver_SOURCES})
target_link_libraries(fpserverlib fpindexlib qhttp PkgConfig::GRPCPP PkgConfig::PROTOBUF)

add_custom_command(
    COMMAND
        protoc
          -Isrc/server/grpc/proto
	  --cpp_out=src/server/grpc/proto
	  --grpc_out=src/server/grpc/proto
          --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin
	  src/server/grpc/proto/index.proto
    OUTPUT
      src/server/grpc/proto/index.pb.h
      src/server/grpc/proto/index.pb.cc
      src/server/grpc/proto/index.grpc.pb.h
      src/server/grpc/proto/index.grpc.pb.cc
    DEPENDS
      src/server/grpc/proto/index.proto
)

add_custom_command(
    COMMAND
	protoc
	  -Isrc/server/grpc/proto
	  --cpp_out=${CMAKE_CURRENT_SOURCE_DIR}/src/server/grpc/proto
	  src/server/grpc/proto/google/api/annotations.proto
	  src/server/grpc/proto/google/api/http.proto
    OUTPUT
      src/server/grpc/proto/google/api/annotations.pb.h
      src/server/grpc/proto/google/api/annotations.pb.cc
      src/server/grpc/proto/google/api/http.pb.h
      src/server/grpc/proto/google/api/http.pb.cc
    DEPENDS
      src/server/grpc/proto/google/api/annotations.proto
      src/server/grpc/proto/google/api/http.proto
)

include_directories(
	${CMAKE_CURRENT_SOURCE_DIR}/src
	${CMAKE_CURRENT_SOURCE_DIR}/src/server/grpc/proto
	${CMAKE_CURRENT_SOURCE_DIR}/src/3rdparty/qhttp/3rdparty
	${CMAKE_CURRENT_SOURCE_DIR}/src/3rdparty/qhttp/src
    ${GTEST_INCLUDE_DIRS}
)

add_executable(fpi-server src/server/main.cpp)
target_link_libraries(fpi-server fpserverlib qhttp SQLite::SQLite3)

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
	src/index/multi_index_test.cpp
	src/index/index_test.cpp
	src/index/index_info_test.cpp
	src/index/index_reader_test.cpp
	src/index/index_writer_test.cpp
	src/index/index_file_deleter_test.cpp
	src/index/segment_enum_test.cpp
	src/index/segment_merger_test.cpp
	src/index/segment_merge_policy_test.cpp
	src/index/op_test.cpp
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
	src/server/http_test.cpp
	src/server/grpc/service_test.cpp
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
