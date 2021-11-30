// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "top_hits_collector.h"

#include <gtest/gtest.h>

#include "util/test_utils.h"

using namespace Acoustid;

TEST(TopHitsCollectorTest, NumHits) {
    TopHitsCollector collector(3);
    collector.collect(1);
    collector.collect(2);
    collector.collect(3);
    collector.collect(4);
    collector.collect(2);
    collector.collect(3);
    collector.collect(4);
    collector.collect(3);
    collector.collect(4);
    collector.collect(4);

    QList<Result> results = collector.topResults();
    ASSERT_EQ(3, results.size());
}

TEST(TopHitsCollectorTest, NumHitsTopScorePercent) {
    TopHitsCollector collector(3, 70);
    collector.collect(1);
    collector.collect(2);
    collector.collect(3);
    collector.collect(4);
    collector.collect(2);
    collector.collect(3);
    collector.collect(4);
    collector.collect(3);
    collector.collect(4);
    collector.collect(4);

    QList<Result> results = collector.topResults();
    ASSERT_EQ(2, results.size());
}

TEST(TopHitsCollectorTest, NumHitsTopScorePercent2) {
    TopHitsCollector collector(3, 90);
    collector.collect(1);
    collector.collect(2);
    collector.collect(3);
    collector.collect(4);
    collector.collect(2);
    collector.collect(3);
    collector.collect(4);
    collector.collect(3);
    collector.collect(4);
    collector.collect(4);

    QList<Result> results = collector.topResults();
    ASSERT_EQ(1, results.size());
}
