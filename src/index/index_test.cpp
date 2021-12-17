// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index.h"

#include <gtest/gtest.h>

#include "store/input_stream.h"
#include "store/output_stream.h"
#include "store/ram_directory.h"
#include "util/test_utils.h"

using namespace Acoustid;

TEST(IndexTest, OpenEmpty) {
    auto dir = QSharedPointer<RAMDirectory>::create();
    ASSERT_THROW({ QSharedPointer<Index>::create(dir); }, IOException);
}

TEST(IndexTest, OpenEmptyCreate) {
    auto dir = QSharedPointer<RAMDirectory>::create();
    auto index = QSharedPointer<Index>::create(dir, true);
    ASSERT_TRUE(index->isOpen());
    index->close();
}

TEST(IndexTest, Insert) {
    auto dir = QSharedPointer<RAMDirectory>::create();
    auto index = QSharedPointer<Index>::create(dir, true);

    ASSERT_FALSE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});

    index->insertOrUpdateDocument(1, {1, 2, 3});
    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{SearchResult(1, 3, 0)});

    index = QSharedPointer<Index>::create(dir, false);

    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{SearchResult(1, 3, 0)});

    index->close();
}

TEST(IndexTest, InsertAndUpdate) {
    auto dir = QSharedPointer<RAMDirectory>::create();
    auto index = QSharedPointer<Index>::create(dir, true);

    ASSERT_FALSE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});

    index->insertOrUpdateDocument(1, {1, 2, 3});
    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{SearchResult(1, 3, 0)});

    index->insertOrUpdateDocument(1, {5, 6, 7});
    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});
    ASSERT_EQ(index->search({5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3, 0)});
    ASSERT_EQ(index->search({1, 2, 3, 4, 5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3, 0)});

    index = QSharedPointer<Index>::create(dir, false);

    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});
    ASSERT_EQ(index->search({5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3, 0)});
    ASSERT_EQ(index->search({1, 2, 3, 4, 5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3, 0)});

    index->close();
}

TEST(IndexTest, InsertAndDelete) {
    auto dir = QSharedPointer<RAMDirectory>::create();
    auto index = QSharedPointer<Index>::create(dir, true);

    ASSERT_FALSE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});

    index->insertOrUpdateDocument(1, {1, 2, 3});
    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{SearchResult(1, 3, 0)});

    index->deleteDocument(1);
    ASSERT_FALSE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});

    index = QSharedPointer<Index>::create(dir, false);

    ASSERT_FALSE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});

    index->close();
}

TEST(IndexTest, InsertAndDeleteAndInsert) {
    auto dir = QSharedPointer<RAMDirectory>::create();
    auto index = QSharedPointer<Index>::create(dir, true);

    ASSERT_FALSE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});

    index->insertOrUpdateDocument(1, {1, 2, 3});
    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{SearchResult(1, 3, 0)});

    index->deleteDocument(1);
    ASSERT_FALSE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});

    index->insertOrUpdateDocument(1, {5, 6, 7});
    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});
    ASSERT_EQ(index->search({5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3, 0)});
    ASSERT_EQ(index->search({1, 2, 3, 4, 5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3, 0)});

    index = QSharedPointer<Index>::create(dir, false);

    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});
    ASSERT_EQ(index->search({5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3, 0)});
    ASSERT_EQ(index->search({1, 2, 3, 4, 5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3, 0)});

    index->close();
}

TEST(IndexTest, Flush) {
    auto dir = QSharedPointer<RAMDirectory>::create();
    auto index = QSharedPointer<Index>::create(dir, true);

    index->insertOrUpdateDocument(1, {1, 2, 3});
    index->insertOrUpdateDocument(1, {5, 6, 7});
    index->flush();

    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});
    ASSERT_EQ(index->search({5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3)});

    index->close();
    index = QSharedPointer<Index>::create(dir, true);

    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});
    ASSERT_EQ(index->search({5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3)});

    index->close();
}

TEST(IndexTest, FlushMerge) {
    auto dir = QSharedPointer<RAMDirectory>::create();
    auto index = QSharedPointer<Index>::create(dir, true);

    for (auto i = 0; i < 100; ++i) {
        index->insertOrUpdateDocument(1, {1, 2, 3});
        index->insertOrUpdateDocument(1, {5, 6, 7});
        index->setAttribute("i", QString::number(i));
        index->flush();
    }

    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});
    ASSERT_EQ(index->search({5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3)});
    ASSERT_EQ(index->getAttribute("i").toStdString(), "99");

    index->close();
    index = QSharedPointer<Index>::create(dir, true);

    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_EQ(index->search({1, 2, 3}), std::vector<SearchResult>{});
    ASSERT_EQ(index->search({5, 6, 7}), std::vector<SearchResult>{SearchResult(1, 3)});
    ASSERT_EQ(index->getAttribute("i").toStdString(), "99");

    index->close();
}
