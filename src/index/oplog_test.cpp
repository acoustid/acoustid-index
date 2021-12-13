// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index/oplog.h"

#include <gtest/gtest.h>

#include "store/ram_directory.h"

using namespace Acoustid;

class OpLogTest : public ::testing::Test {
 protected:
    void SetUp() {
        dir = std::make_unique<RAMDirectory>();
        oplog = std::make_unique<OpLog>(dir->openDatabase("test.db"));
    }

    void TearDown() {
        oplog = nullptr;
        dir = nullptr;
    }

    std::unique_ptr<RAMDirectory> dir;
    std::unique_ptr<OpLog> oplog;
};

TEST_F(OpLogTest, ReadWrite) {
    OpBatch batch;
    batch.insertOrUpdateDocument(1, {100, 101, 102});
    batch.insertOrUpdateDocument(2, {200, 201, 202});
    batch.deleteDocument(3);
    oplog->write(batch);

    std::vector<OpLogEntry> entries;
    oplog->read(entries, 10);

    ASSERT_EQ(3, entries.size());

    ASSERT_EQ(1, entries.at(0).id());
    ASSERT_EQ(INSERT_OR_UPDATE_DOCUMENT, entries.at(0).op().type());

    ASSERT_EQ(2, entries.at(1).id());
    ASSERT_EQ(INSERT_OR_UPDATE_DOCUMENT, entries.at(1).op().type());

    ASSERT_EQ(3, entries.at(2).id());
    ASSERT_EQ(DELETE_DOCUMENT, entries.at(2).op().type());
}
