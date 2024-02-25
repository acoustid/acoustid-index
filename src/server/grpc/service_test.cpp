#include <gtest/gtest.h>

#include "server/grpc/service.h"
#include "store/ram_directory.h"

using namespace Acoustid;
using namespace Acoustid::Server;

using namespace fpindex;

class IndexService : public ::testing::Test {
 protected:
    void SetUp() override {
        dir = QSharedPointer<RAMDirectory>::create();
        indexes = QSharedPointer<MultiIndex>::create(dir);
        metrics = QSharedPointer<Metrics>::create();
	service = QSharedPointer<IndexServiceImpl>::create(indexes, metrics);
    }

    void TearDown() override {}

 protected:
    QSharedPointer<RAMDirectory> dir;
    QSharedPointer<MultiIndex> indexes;
    QSharedPointer<Metrics> metrics;
    QSharedPointer<IndexServiceImpl> service;
};

TEST_F(IndexService, ListIndexes_Empty) {
    ListIndexesRequest request;
    ListIndexesResponse response;
    grpc::ServerContext context;

    auto status = service->ListIndexes(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(0, response.indexes_size());
}

TEST_F(IndexService, ListIndexes_OneIndex) {
    ListIndexesRequest request;
    ListIndexesResponse response;
    grpc::ServerContext context;

    indexes->createIndex("test");

    auto status = service->ListIndexes(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(1, response.indexes_size());
    EXPECT_EQ("test", response.indexes(0).name());
}

TEST_F(IndexService, ListIndexes_LegacyIndex) {
    ListIndexesRequest request;
    ListIndexesResponse response;
    grpc::ServerContext context;

    indexes->createIndex("test");
    indexes->createRootIndex();

    auto status = service->ListIndexes(&context, &request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(2, response.indexes_size());
    EXPECT_EQ(MultiIndex::ROOT_INDEX_NAME, response.indexes(0).name());
    EXPECT_EQ("test", response.indexes(1).name());
}

