// Copyright (C) 2020  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "server/session.h"

#include <gtest/gtest.h>

#include "index/index.h"
#include "server/metrics.h"
#include "store/ram_directory.h"

using namespace Acoustid;
using namespace Acoustid::Server;

TEST(SessionTest, Attributes) {
    auto storage = QSharedPointer<RAMDirectory>::create();
    auto index = QSharedPointer<Index>::create(storage, true);
    auto metrics = QSharedPointer<Metrics>::create();
    auto session = QSharedPointer<Session>::create(index, metrics);

    ASSERT_EQ("", session->getAttribute("foo").toStdString());
    session->begin();
    session->setAttribute("foo", "bar");
    ASSERT_EQ("bar", session->getAttribute("foo").toStdString());
    session->commit();
    ASSERT_EQ("bar", session->getAttribute("foo").toStdString());

    ASSERT_EQ("500", session->getAttribute("max_results").toStdString());
    session->setAttribute("max_results", "100");
    ASSERT_EQ("100", session->getAttribute("max_results").toStdString());

    ASSERT_EQ("10", session->getAttribute("top_score_percent").toStdString());
    session->setAttribute("top_score_percent", "100");
    ASSERT_EQ("100", session->getAttribute("top_score_percent").toStdString());

    ASSERT_EQ("0", session->getAttribute("timeout").toStdString());
    session->setAttribute("timeout", "100");
    ASSERT_EQ("100", session->getAttribute("timeout").toStdString());
}

TEST(SessionTest, InsertAndSearch) {
    auto storage = QSharedPointer<RAMDirectory>::create();
    auto index = QSharedPointer<Index>::create(storage, true);
    auto metrics = QSharedPointer<Metrics>::create();
    auto session = QSharedPointer<Session>::create(index, metrics);

    session->begin();
    session->insertOrUpdateDocument(1, {1, 2, 3});
    session->insertOrUpdateDocument(2, {1, 200, 300});
    session->commit();

    {
        auto results = session->search({1, 2, 3});
        ASSERT_EQ(2, results.size());
        ASSERT_EQ(1, results[0].docId());
        ASSERT_EQ(3, results[0].score());
        ASSERT_EQ(2, results[1].docId());
        ASSERT_EQ(1, results[1].score());
    }

    {
        auto results = session->search({1, 200, 300});
        ASSERT_EQ(2, results.size());
        ASSERT_EQ(2, results[0].docId());
        ASSERT_EQ(3, results[0].score());
        ASSERT_EQ(1, results[1].docId());
        ASSERT_EQ(1, results[1].score());
    }

    session->setAttribute("max_results", "1");

    {
        auto results = session->search({1, 2, 3});
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results[0].docId());
        ASSERT_EQ(3, results[0].score());
    }
}
