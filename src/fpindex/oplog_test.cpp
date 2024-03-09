#include "fpindex/oplog.h"

#include <gtest/gtest.h>

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

    std::vector<OplogEntry> entries;

    {
        auto& entry = entries.emplace_back();
        auto set_attribute_request = entry.mutable_data()->mutable_set_attribute();
        set_attribute_request->set_key("key");
        set_attribute_request->set_value("value");
    }

    {
        auto& entry = entries.emplace_back();
        auto set_attribute_request = entry.mutable_data()->mutable_set_attribute();
        set_attribute_request->set_key("key2");
        set_attribute_request->set_value("value2");
    }

    ASSERT_TRUE(oplog.Write(entries));

    ASSERT_EQ(1, entries[0].id());
    ASSERT_EQ(2, entries[1].id());
}
