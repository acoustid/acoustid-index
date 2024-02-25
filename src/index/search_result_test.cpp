// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "search_result.h"

using namespace Acoustid;

namespace Acoustid {

std::ostream& operator<<(std::ostream& stream, const SearchResult & result) {
  return stream << "SearchResult(docId=" << result.docId() << ", score=" << result.score() << ", version=" << result.version() <<")";
} 

}

TEST(SearchResultTest, SortSearchResults) {
    std::vector<SearchResult> results = {
        { 100, 1, 0 },
        { 101, 1, 0 },
        { 101, 10, 0 },
    };
    sortSearchResults(results);
    std::vector<SearchResult> expected = {
        { 101, 10, 0 },
        { 100, 1, 0 },
        { 101, 1, 0 },
    };
    ASSERT_EQ(results, expected);
}

TEST(SearchResultTest, FilterSearchResultsEmpty) {
    std::vector<SearchResult> results;
    sortSearchResults(results);
    filterSearchResults(results, 2);
    std::vector<SearchResult> expected;
    ASSERT_EQ(results, expected);
}

TEST(SearchResultTest, FilterSearchResultsLimit) {
    std::vector<SearchResult> results = {
        { 100, 1, 0 },
        { 101, 1, 0 },
        { 101, 10, 0 },
    };
    sortSearchResults(results);
    filterSearchResults(results, 2);
    std::vector<SearchResult> expected = {
        { 101, 10, 0 },
        { 100, 1, 0 },
    };
    ASSERT_EQ(results, expected);
}

TEST(SearchResultTest, FilterSearchResultsMinScore1) {
    std::vector<SearchResult> results = {
        { 100, 1, 0 },
        { 101, 1, 0 },
        { 101, 10, 0 },
    };
    sortSearchResults(results);
    filterSearchResults(results, 10, 1);
    std::vector<SearchResult> expected = {
        { 101, 10, 0 },
        { 100, 1, 0 },
        { 101, 1, 0 },
    };
    ASSERT_EQ(results, expected);
}

TEST(SearchResultTest, FilterSearchResultsMinScore90) {
    std::vector<SearchResult> results = {
        { 100, 1, 0 },
        { 101, 1, 0 },
        { 101, 10, 0 },
    };
    sortSearchResults(results);
    filterSearchResults(results, 10, 90);
    std::vector<SearchResult> expected = {
        { 101, 10, 0 },
    };
    ASSERT_EQ(results, expected);
}
