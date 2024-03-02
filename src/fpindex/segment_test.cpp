#include "fpindex/segment.h"

#include <gtest/gtest.h>

using namespace fpindex;
using namespace testing;

namespace fpindex {

void SortSearchResults(std::vector<SearchResult>* results) {
    std::sort(results->begin(), results->end(), [](const SearchResult& a, const SearchResult& b) {
        return a.score() > b.score() || (a.score() == b.score() && a.id() < b.id());
    });
}

std::ostream& operator<<(std::ostream& os, const SearchResult& result) {
    os << "SearchResult(" << result.id() << ", " << result.score() << ")";
    return os;
}

std::ostream& operator<<(std::ostream& os, const std::vector<SearchResult>& results) {
    os << "SearchResults(";
    for (const auto& result : results) {
        os << result << ", ";
    }
    os << ")";
    return os;
}

}  // namespace fpindex

class MockSegment : public BlockBasedSegment {
 public:
    MockSegment() : BlockBasedSegment(0) {}

    const std::vector<std::pair<uint32_t, uint32_t>>& GetBlockIndex() override { return block_index_; }

    bool GetBlock(size_t block_no, std::vector<std::pair<uint32_t, uint32_t>>* block) override {
        if (block_no < block_index_.size()) {
            block->assign(blocks_[block_no].begin(), blocks_[block_no].end());
            return true;
        }
        return false;
    }

    void AddBlock(const std::vector<std::pair<uint32_t, uint32_t>>& block) {
        blocks_.push_back(block);
        block_index_.emplace_back(block.front().first, block.back().first);
    }

    std::vector<std::pair<uint32_t, uint32_t>> block_index_;
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> blocks_;
};

TEST(BlockBasedSegmentTest, SearchEmpty) {
    MockSegment segment;

    std::vector<uint32_t> query{1, 2, 3};
    std::vector<SearchResult> results;
    ASSERT_TRUE(segment.Search(query, &results));
    ASSERT_TRUE(results.empty()) << "results: " << results;
}

TEST(BlockBasedSegmentTest, SearchOneBlockSimple) {
    MockSegment segment;
    segment.AddBlock({{1, 1}, {2, 1}, {3, 1}});

    std::vector<uint32_t> query{1, 2, 3};
    std::vector<SearchResult> results;
    ASSERT_TRUE(segment.Search(query, &results));
    ASSERT_FALSE(results.empty()) << "results: " << results;
    ASSERT_EQ(1, results.size());
    ASSERT_EQ(1, results[0].id());
    ASSERT_EQ(3, results[0].score());
}

TEST(BlockBasedSegmentTest, SearchMultipleBlocksSimple) {
    MockSegment segment;
    segment.AddBlock({{1, 1}, {2, 1}});
    segment.AddBlock({{3, 1}});

    std::vector<uint32_t> query{1, 2, 3};
    std::vector<SearchResult> results;
    ASSERT_TRUE(segment.Search(query, &results));
    ASSERT_FALSE(results.empty()) << "results: " << results;
    ASSERT_EQ(1, results.size());
    ASSERT_EQ(1, results[0].id());
    ASSERT_EQ(3, results[0].score());
}

TEST(BlockBasedSegmentTest, SearchOneHashAcrossMultipleBlocks) {
    MockSegment segment;
    segment.AddBlock({{1, 1}, {1, 2}, {1, 3}});
    segment.AddBlock({{1, 4}, {2, 1}, {3, 1}});

    std::vector<uint32_t> query{1, 2, 3};
    std::vector<SearchResult> results;
    ASSERT_TRUE(segment.Search(query, &results));
    ASSERT_FALSE(results.empty()) << "results: " << results;
    ASSERT_EQ(4, results.size());
    SortSearchResults(&results);
    ASSERT_EQ(1, results[0].id());
    ASSERT_EQ(3, results[0].score());
    ASSERT_EQ(2, results[1].id());
    ASSERT_EQ(1, results[1].score());
    ASSERT_EQ(3, results[2].id());
    ASSERT_EQ(1, results[2].score());
    ASSERT_EQ(4, results[3].id());
    ASSERT_EQ(1, results[3].score());
}
