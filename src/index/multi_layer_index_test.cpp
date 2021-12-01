// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "multi_layer_index.h"

#include <gtest/gtest.h>

#include "store/ram_directory.h"

using namespace Acoustid;

TEST(MultiLayerIndex, Open) {
    auto dir = QSharedPointer<RAMDirectory>::create();
    auto index = QSharedPointer<MultiLayerIndex>::create();

    index->open(dir, true);

    ASSERT_FALSE(index->hasAttribute("foo"));
    ASSERT_EQ(index->getAttribute("foo"), "");

    index->setAttribute("foo", "bar");

    ASSERT_TRUE(index->hasAttribute("foo"));
    ASSERT_EQ(index->getAttribute("foo"), "bar");
}
