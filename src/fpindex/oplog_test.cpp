#include "fpindex/oplog.h"

#include <gtest/gtest.h>

#include "fpindex/io/memory_directory.h"
#include "fpindex/update.h"

using namespace fpindex;

TEST(OplogTest, Open) {
    auto dir = std::make_shared<io::MemoryDirectory>();
    auto db = dir->OpenDatabase("index.db", true);

    Oplog oplog(db);
    ASSERT_TRUE(oplog.Open());

    ASSERT_TRUE(db->Close());
}

TEST(OplogTest, Write) {
    auto dir = std::make_shared<io::MemoryDirectory>();
    auto db = dir->OpenDatabase("index.db", true);

    Oplog oplog(db);
    ASSERT_TRUE(oplog.Open());

    IndexUpdate update;
    update.InsertOrUpdate(1, {1, 2, 3});
    update.SetAttribute("txn_id", "123456789");

    auto entries = update.Finish();
    ASSERT_EQ(2, entries.size());
    ASSERT_EQ(0, entries[0].id());
    ASSERT_EQ(0, entries[1].id());

    ASSERT_TRUE(oplog.Write(entries));
    ASSERT_EQ(1, entries[0].id());
    ASSERT_EQ(2, entries[1].id());

    ASSERT_TRUE(db->Close());
}
