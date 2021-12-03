// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "multi_index.h"

#include <gtest/gtest.h>

#include "store/ram_directory.h"

using namespace Acoustid;

TEST(MultiIndexTest, IndexExists) {
    auto dir = QSharedPointer<RAMDirectory>::create();
    auto multiIndex = QSharedPointer<MultiIndex>::create(dir);

    ASSERT_FALSE(multiIndex->indexExists("idx01"));
}
