# Locate the Google C++ Testing Framework source directory.
#
# Defines the following variables:
#
#   GTEST_FOUND - Found the Google Testing framework sources
#   GTEST_INCLUDE_DIRS - Include directories
#   GTEST_SOURCE_DIR - Source code directory
#   GTEST_LIBRARIES - libgtest
#   GTEST_MAIN_LIBRARIES - libgtest-main
#   GTEST_BOTH_LIBRARIES - libgtest & libgtest-main
#
# Accepts the following variables as input:
#
#   GTEST_ROOT - (as CMake or environment variable)
#                The root directory of the gtest install prefix
#
# Example usage:
#
#    find_package(GTest REQUIRED)
#    include_directories(${GTEST_INCLUDE_DIRS})
#    add_subdirectory(${GTEST_SOURCE_DIR}
#        ${CMAKE_CURRENT_BINARY_DIR}/gtest_build)
#
#    add_executable(foo foo.cc)
#    target_link_libraries(foo ${GTEST_BOTH_LIBRARIES})
#
#    enable_testing(true)
#    add_test(AllTestsInFoo foo)


find_path(GTEST_SOURCE_DIR
	NAMES src/gtest-all.cc CMakeLists.txt
	HINTS $ENV{GTEST_ROOT} ${GTEST_ROOT} /usr/src/gtest
)
mark_as_advanced(GTEST_SOURCE_DIR)

find_path(GTEST_INCLUDE_DIR
	NAMES gtest/gtest.h
	HINTS $ENV{GTEST_ROOT}/include ${GTEST_ROOT}/include
)
mark_as_advanced(GTEST_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GTest DEFAULT_MSG GTEST_SOURCE_DIR GTEST_INCLUDE_DIR)

if(GTEST_FOUND)
	set(GTEST_INCLUDE_DIRS ${GTEST_INCLUDE_DIR})
	set(GTEST_LIBRARIES gtest)
	set(GTEST_MAIN_LIBRARIES gtest_main)
    set(GTEST_BOTH_LIBRARIES ${GTEST_LIBRARIES} ${GTEST_MAIN_LIBRARIES})
    set(GTEST_SOURCES ${GTEST_SOURCE_DIR}/src/gtest-all.cc)
    set(GTEST_MAIN_SOURCES ${GTEST_SOURCE_DIR}/src/gtest_main.cc)
endif()
