// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>

#include "in_memory_index.h"
#include "top_hits_collector.h"

using namespace Acoustid;

TEST(InMemoryIndexTest, Attributes)
{
    auto index = QSharedPointer<InMemoryIndex>::create();

    ASSERT_FALSE(index->hasAttribute("foo"));
    ASSERT_EQ(index->getAttribute("foo"), "");

    index->setAttribute("foo", "bar");

    ASSERT_TRUE(index->hasAttribute("foo"));
    ASSERT_EQ(index->getAttribute("foo"), "bar");
}

TEST(InMemoryIndexTest, Documents)
{
    auto index = QSharedPointer<InMemoryIndex>::create();

    ASSERT_FALSE(index->containsDocument(1));
    ASSERT_FALSE(index->isDocumentDeleted(1));
    ASSERT_FALSE(index->deleteDocument(1));
    ASSERT_TRUE(index->isDocumentDeleted(1));
    ASSERT_FALSE(index->insertOrUpdateDocument(1, {100, 200, 300}));

    ASSERT_TRUE(index->containsDocument(1));
    ASSERT_TRUE(index->insertOrUpdateDocument(1, {101, 201, 301}));
    ASSERT_TRUE(index->deleteDocument(1));

    ASSERT_FALSE(index->insertOrUpdateDocument(1, {101, 201, 301}));
    ASSERT_FALSE(index->insertOrUpdateDocument(2, {102, 202, 302}));
    ASSERT_FALSE(index->insertOrUpdateDocument(3, {103, 203, 303}));

    auto collector = QSharedPointer<TopHitsCollector>::create(100);
    index->search({101, 201, 303}, collector.get());

    auto results = collector->topResults();
    ASSERT_EQ(results.size(), 2);

    ASSERT_EQ(results[0].id(), 1);
    ASSERT_EQ(results[0].score(), 2);

    ASSERT_EQ(results[1].id(), 3);
    ASSERT_EQ(results[1].score(), 1);
}
