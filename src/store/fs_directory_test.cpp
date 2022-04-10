// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "fs_directory.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

using namespace Acoustid;

TEST(FSDirectory, OpenDatabase) {
    auto dir = std::unique_ptr<FSDirectory>(FSDirectory::openTemporary(true));
    auto db = dir->openDatabase("foo.db");
}
