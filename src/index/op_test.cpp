// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "index/op.h"

#include <gtest/gtest.h>

#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>

using namespace Acoustid;

TEST(OpTest, InsertOrUpdateDocumentJson) {
    auto op = Op(InsertOrUpdateDocument(1, {101, 102, 103}));

    auto opJson = QJsonDocument(op.toJson()).toJson(QJsonDocument::Compact);
    ASSERT_EQ(opJson.toStdString(), "{\"upsert\":{\"id\":1,\"terms\":[101,102,103]}}");

    auto op2 = Op::fromJson(QJsonDocument::fromJson(opJson).object());
    ASSERT_EQ(op, op2);
}

TEST(OpTest, DeleteDocumentJson) {
    auto op = Op(DeleteDocument(1));

    auto opJson = QJsonDocument(op.toJson()).toJson(QJsonDocument::Compact);
    ASSERT_EQ(opJson.toStdString(), "{\"delete\":{\"id\":1}}");

    auto op2 = Op::fromJson(QJsonDocument::fromJson(opJson).object());
    ASSERT_EQ(op, op2);
}

TEST(OpTest, SetAttributeJson) {
    auto op = Op(SetAttribute("foo", "bar"));

    auto opJson = QJsonDocument(op.toJson()).toJson(QJsonDocument::Compact);
    ASSERT_EQ(opJson.toStdString(), "{\"set\":{\"name\":\"foo\",\"value\":\"bar\"}}");

    auto op2 = Op::fromJson(QJsonDocument::fromJson(opJson).object());
    ASSERT_EQ(op, op2);
}
