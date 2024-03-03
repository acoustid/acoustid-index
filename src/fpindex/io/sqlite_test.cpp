#include "fpindex/io/sqlite.h"

#include <gtest/gtest.h>

using namespace fpindex;

TEST(SqliteTest, OpenClose) {
    auto db = io::OpenDatabase(":memory:");
    EXPECT_TRUE(db);
    io::CloseDatabase(db);
    EXPECT_FALSE(db);
}

TEST(SqliteTest, OpenAutoClose) {
    auto db = io::OpenDatabase(":memory:");
    EXPECT_TRUE(db);
}
