#include "fpindex/segment_builder.h"

#include <gtest/gtest.h>

#include "fpindex/io/memory_file.h"
#include "fpindex/segment.h"

using namespace fpindex;
using namespace testing;

TEST(SegmentBuilderTest, SearchEmpty) {
    SegmentBuilder segment(0);

    std::vector<uint32_t> query{1, 2, 3};
    std::vector<SearchResult> results;
    ASSERT_TRUE(segment.Search(query, &results));
    ASSERT_TRUE(results.empty());
}

TEST(SegmentBuilderTest, SearchExactMatch) {
    SegmentBuilder segment(0);
    segment.Add(1, {1, 2, 3});

    std::vector<uint32_t> query{1, 2, 3};
    std::vector<SearchResult> results;
    ASSERT_TRUE(segment.Search(query, &results));
    ASSERT_EQ(1, results.size());
    ASSERT_EQ(1, results[0].id());
    ASSERT_EQ(3, results[0].score());
}

TEST(SegmentBuilderTest, Save) {
    SegmentBuilder segment(0);
    segment.Add(1, {1, 2, 3});

    auto file = std::make_shared<io::MemoryFile>();
    auto segment2 = segment.Save(file);

    {
        std::vector<uint32_t> query{1, 2, 3};
        std::vector<SearchResult> results;
        ASSERT_TRUE(segment2->Search(query, &results));
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results[0].id());
        ASSERT_EQ(3, results[0].score());
    }

    Segment segment3(0);
    segment3.Load(file);

    {
        std::vector<uint32_t> query{1, 2, 3};
        std::vector<SearchResult> results;
        ASSERT_TRUE(segment3.Search(query, &results));
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results[0].id());
        ASSERT_EQ(3, results[0].score());
    }
}
