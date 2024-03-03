#include "fpindex/util/cleanup.h"

#include <gtest/gtest.h>

using namespace fpindex;

TEST(CleanupTest, Cleanup) {
    std::string s = "initial";
    {
        auto cleanup = util::MakeCleanup([&s]() {
            s = "cleaned";
        });
        EXPECT_EQ(s, "initial");
    }
    EXPECT_EQ(s, "cleaned");
}

TEST(CleanupTest, Invoke) {
    std::string s = "initial";
    {
        auto cleanup = util::MakeCleanup([&s]() {
            s = "cleaned";
        });
        EXPECT_EQ(s, "initial");
        cleanup.Invoke();
        EXPECT_EQ(s, "cleaned");
        s = "updated";
    }
    EXPECT_EQ(s, "updated");
}

TEST(CleanupTest, Cancel) {
    std::string s = "initial";
    {
        auto cleanup = util::MakeCleanup([&s]() {
            s = "cleaned";
        });
        cleanup.Cancel();
        EXPECT_EQ(s, "initial");
    }
    EXPECT_EQ(s, "initial");
}
