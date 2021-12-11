// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>

#include <QSqlQuery>

#include "fs_directory.h"

using namespace Acoustid;

TEST(FSDirectory, OpenDatabase) {
    auto dir = QSharedPointer<FSDirectory>(FSDirectory::openTemporary(true));
    auto db = dir->openDatabase("foo.db");

    QSqlQuery query(db);
    query.exec("CREATE TABLE foo (a int)");
    query.exec("INSERT INTO foo (a) VALUES (1)");
    query.exec("SELECT * FROM foo");

    ASSERT_TRUE(query.first());
    ASSERT_EQ(query.value(0).toInt(), 1);
}
