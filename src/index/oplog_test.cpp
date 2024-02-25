// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index/oplog.h"

#include <gtest/gtest.h>

#include "store/ram_directory.h"

using namespace Acoustid;

class OplogTest : public ::testing::Test {
 protected:
    void SetUp() {
        dir = std::make_unique<RAMDirectory>();
        oplog = std::make_unique<Oplog>(dir->openDatabase("test.db"));
    }

    void TearDown() {
        oplog = nullptr;
        dir = nullptr;
    }

    std::unique_ptr<RAMDirectory> dir;
    std::unique_ptr<Oplog> oplog;
};

TEST_F(OplogTest, ReadWrite) {
    OpBatch batch;
    batch.insertOrUpdateDocument(1, {100, 101, 102});
    batch.insertOrUpdateDocument(2, {200, 201, 202});
    batch.deleteDocument(3);
    oplog->write(batch);

    std::vector<OplogEntry> entries;
    oplog->read(entries, 10);

    ASSERT_EQ(3, entries.size());

    ASSERT_EQ(1, entries.at(0).id());
    ASSERT_EQ(INSERT_OR_UPDATE_DOCUMENT, entries.at(0).op().type());

    ASSERT_EQ(2, entries.at(1).id());
    ASSERT_EQ(INSERT_OR_UPDATE_DOCUMENT, entries.at(1).op().type());

    ASSERT_EQ(3, entries.at(2).id());
    ASSERT_EQ(DELETE_DOCUMENT, entries.at(2).op().type());
}

TEST_F(OplogTest, GetLastOpId) {
    ASSERT_EQ(0, oplog->getLastOpId());

    OpBatch batch1;
    batch1.insertOrUpdateDocument(1, {100, 101, 102});
    batch1.insertOrUpdateDocument(2, {200, 201, 202});
    batch1.deleteDocument(3);
    oplog->write(batch1);

    ASSERT_EQ(3, oplog->getLastOpId());

    OpBatch batch2;
    batch2.insertOrUpdateDocument(1, {1000, 1001, 1002});
    oplog->write(batch2);

    ASSERT_EQ(4, oplog->getLastOpId());
}

TEST_F(OplogTest, CreateReplicationSlot) {
    ASSERT_THROW(oplog->getLastOpId("test"), ReplicationSlotDoesNotExistError);

    oplog->createReplicationSlot("test");
    ASSERT_EQ(0, oplog->getLastOpId("test"));
}

TEST_F(OplogTest, CreateReplicationSlotDuplicateSlot) {
    oplog->createReplicationSlot("test");
    ASSERT_THROW(oplog->createReplicationSlot("test"), ReplicationSlotAlreadyExistsError);
}

TEST_F(OplogTest, CreateOrUpdateReplicationSlot) {
    oplog->createOrUpdateReplicationSlot("test", 0);
    oplog->createOrUpdateReplicationSlot("test", 0);
}

TEST_F(OplogTest, DeleteReplicationSlot) {
    oplog->createReplicationSlot("test");
    ASSERT_EQ(0, oplog->getLastOpId("test"));

    oplog->deleteReplicationSlot("test");
    ASSERT_THROW(oplog->getLastOpId("test"), ReplicationSlotDoesNotExistError);
}

TEST_F(OplogTest, DeleteReplicationSlotInvalidSlot) {
    ASSERT_THROW(oplog->deleteReplicationSlot("test"), ReplicationSlotDoesNotExistError);
}

TEST_F(OplogTest, UpdateReplicationSlot) {
    oplog->createReplicationSlot("test");
    ASSERT_EQ(0, oplog->getLastOpId("test"));

    OpBatch batch1;
    batch1.insertOrUpdateDocument(1, {1000, 1001, 1002});
    oplog->write(batch1);

    ASSERT_EQ(1, oplog->getLastOpId());
    ASSERT_EQ(0, oplog->getLastOpId("test"));

    oplog->updateReplicationSlot("test", 1);
    ASSERT_EQ(1, oplog->getLastOpId("test"));
}

TEST_F(OplogTest, UpdateReplicationSlotInvalidSlot) {
    OpBatch batch1;
    batch1.insertOrUpdateDocument(1, {1000, 1001, 1002});
    oplog->write(batch1);

    ASSERT_EQ(1, oplog->getLastOpId());

    ASSERT_THROW(oplog->updateReplicationSlot("test", 1), ReplicationSlotDoesNotExistError);
}

TEST_F(OplogTest, UpdateReplicationSlotInvalidOpId) {
    oplog->createReplicationSlot("test");

    ASSERT_EQ(0, oplog->getLastOpId());

    ASSERT_THROW(oplog->updateReplicationSlot("test", 1), OpDoesNotExistError);
}
