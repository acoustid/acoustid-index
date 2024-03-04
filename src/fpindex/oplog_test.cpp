#include <gtest/gtest.h>

#include "fpindex/oplog.h"
#include "fpindex/io/sqlite.h"

using namespace fpindex;

TEST(OplogTest, Open) {
    auto db = io::OpenDatabase(":memory:", true);
    Oplog oplog(db);
    ASSERT_TRUE(oplog.Open());
}

TEST(OplogTest, Write) {
    auto db = io::OpenDatabase(":memory:", true);
    Oplog oplog(db);
    ASSERT_TRUE(oplog.Open());

    OplogEntries entries;
    auto entry = entries.add_entries();
    entry->set_op_id(1);
    entry->mutable_set_attribute()->set_key("key");
    entry->mutable_set_attribute()->set_value("value");

    ASSERT_TRUE(oplog.Write(entries));
}