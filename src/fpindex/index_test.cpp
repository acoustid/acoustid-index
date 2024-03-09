#include "fpindex/index.h"

#include <gtest/gtest.h>

#include "fpindex/io/memory_directory.h"

using namespace fpindex;
using namespace testing;

TEST(IndexTest, Update) {
    auto dir = std::make_shared<io::MemoryDirectory>();
    auto index = std::make_shared<Index>(dir);
    ASSERT_TRUE(index->Open());

    IndexUpdate batch;
    batch.InsertOrUpdate(1, {1, 2, 3});

    EXPECT_TRUE(index->Update(std::move(batch)));

    ASSERT_TRUE(index->Close());
}
