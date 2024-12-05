#include "server/http.h"

#include <gtest/gtest.h>

#include <QJsonArray>
#include <QJsonObject>

#include "index/index.h"
#include "server/metrics.h"
#include "store/ram_directory.h"

using namespace Acoustid;
using namespace Acoustid::Server;

class HttpTest : public ::testing::Test {
 protected:
    void SetUp() override {
        dir = QSharedPointer<RAMDirectory>::create();
        index = QSharedPointer<Index>::create(dir, true);
        metrics = QSharedPointer<Metrics>::create();
        handler = QSharedPointer<HttpRequestHandler>::create(index, metrics);
    }

    void TearDown() override {
        handler.clear();
        metrics.clear();
        index.clear();
        dir.clear();
    }

 protected:
    QSharedPointer<RAMDirectory> dir;
    QSharedPointer<Index> index;
    QSharedPointer<Metrics> metrics;
    QSharedPointer<HttpRequestHandler> handler;
};

TEST_F(HttpTest, TestReady) {
    auto request = HttpRequest(HTTP_GET, QUrl("/_health/ready"));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_OK);
    ASSERT_EQ(response.body().toStdString(), "OK\n");
}

TEST_F(HttpTest, TestAlive) {
    auto request = HttpRequest(HTTP_GET, QUrl("/_health/alive"));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_OK);
    ASSERT_EQ(response.body().toStdString(), "OK\n");
}

TEST_F(HttpTest, TestMetrics) {
    auto request = HttpRequest(HTTP_GET, QUrl("/_metrics"));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_OK);
    ASSERT_EQ(response.header("Content-Type").toStdString(), "text/plain; version=0.0.4");
}

TEST_F(HttpTest, TestHeadIndex) {
    auto request = HttpRequest(HTTP_HEAD, QUrl("/main"));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_OK);
    ASSERT_EQ(response.body().toStdString(), "{}");
}

TEST_F(HttpTest, TestHeadIndexNotFound) {
    auto request = HttpRequest(HTTP_HEAD, QUrl("/foo"));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_NOT_FOUND);
    ASSERT_EQ(response.body().toStdString(),
              "{\"error\":{\"description\":\"index does not exist\",\"type\":\"not_found\"},\"status\":404}");
}

TEST_F(HttpTest, TestGetIndex) {
    auto request = HttpRequest(HTTP_GET, QUrl("/main"));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_OK);
    ASSERT_EQ(response.body().toStdString(), "{\"revision\":0}");
}

TEST_F(HttpTest, TestGetIndexNotFound) {
    auto request = HttpRequest(HTTP_GET, QUrl("/foo"));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_NOT_FOUND);
    ASSERT_EQ(response.body().toStdString(),
              "{\"error\":{\"description\":\"index does not exist\",\"type\":\"not_found\"},\"status\":404}");
}

TEST_F(HttpTest, TestPutDocumentStringTerms) {
    auto request = HttpRequest(HTTP_PUT, QUrl("/main/111"));
    request.setBody(QJsonDocument(QJsonObject{{"hashes", "1,2,3"}}));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_OK);
    ASSERT_EQ(response.body().toStdString(), "{}");
}

TEST_F(HttpTest, TestPutDocumentArrayTerms) {
    auto request = HttpRequest(HTTP_PUT, QUrl("/main/111"));
    request.setBody(QJsonDocument(QJsonObject{{"hashes", QJsonArray{1, 2, 3}}}));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_OK);
    ASSERT_EQ(response.body().toStdString(), "{}");
}

/*TEST_F(HttpTest, TestSearch) {
    indexes->createIndex("testidx");
    indexes->getIndex("testidx")->insertOrUpdateDocument(111, {1, 2, 3});
    indexes->getIndex("testidx")->insertOrUpdateDocument(112, {3, 4, 5});

    auto request = HttpRequest(HTTP_GET, QUrl("/testidx/_search?query=1,2,3"));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_OK);
    ASSERT_EQ(response.body().toStdString(), "{\"results\":[{\"id\":111,\"score\":3},{\"id\":112,\"score\":1}]}");
}

TEST_F(HttpTest, TestSearchLimit) {
    indexes->createIndex("testidx");
    indexes->getIndex("testidx")->insertOrUpdateDocument(111, {1, 2, 3});
    indexes->getIndex("testidx")->insertOrUpdateDocument(112, {3, 4, 5});

    auto request = HttpRequest(HTTP_GET, QUrl("/testidx/_search?query=1,2,3&limit=1"));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_OK);
    ASSERT_EQ(response.body().toStdString(), "{\"results\":[{\"id\":111,\"score\":3}]}");
}

TEST_F(HttpTest, TestSearchNoResults) {
    indexes->createIndex("testidx");
    indexes->getIndex("testidx")->insertOrUpdateDocument(111, {1, 2, 3});
    indexes->getIndex("testidx")->insertOrUpdateDocument(112, {3, 4, 5});

    auto request = HttpRequest(HTTP_GET, QUrl("/testidx/_search?query=7,8,9&limit=1"));
    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_OK);
    ASSERT_EQ(response.body().toStdString(), "{\"results\":[]}");
}

TEST_F(HttpTest, TestBulkArray) {
    indexes->createIndex("testidx");
    indexes->getIndex("testidx")->insertOrUpdateDocument(112, {31, 41, 51});
    indexes->getIndex("testidx")->insertOrUpdateDocument(113, {31, 41, 51});

    auto request = HttpRequest(HTTP_POST, QUrl("/testidx/_bulk"));
    request.setBody(QJsonDocument(QJsonArray{
        QJsonObject{{"upsert", QJsonObject{{"id", 111}, {"terms", QJsonArray{1, 2, 3}}}}},
        QJsonObject{{"upsert", QJsonObject{{"id", 112}, {"terms", QJsonArray{3, 4, 5}}}}},
        QJsonObject{{"delete", QJsonObject{{"id", 113}}}},
        QJsonObject{{"set", QJsonObject{{"name", "foo"}, {"value", "bar"}}}},
    }));

    auto response = handler->router().handle(request);
    ASSERT_EQ(response.status(), HTTP_OK);
    ASSERT_EQ(response.body().toStdString(), "{}");

    ASSERT_TRUE(indexes->getIndex("testidx")->containsDocument(111));
    ASSERT_TRUE(indexes->getIndex("testidx")->containsDocument(112));
    ASSERT_FALSE(indexes->getIndex("testidx")->containsDocument(113));
    ASSERT_EQ(indexes->getIndex("testidx")->getAttribute("foo").toStdString(), "bar");
}*/

TEST_F(HttpTest, TestBulkObject) {
    auto request = HttpRequest(HTTP_POST, QUrl("/main/_update"));
    request.setBody(QJsonDocument(QJsonObject{
        {"changes", QJsonArray{
            QJsonObject{{"insert", QJsonObject{{"id", 111}, {"hashes", QJsonArray{1, 2, 3}}}}},
            QJsonObject{{"insert", QJsonObject{{"id", 112}, {"hashes", QJsonArray{3, 4, 5}}}}},
            QJsonObject{{"set_attribute", QJsonObject{{"name", "foo"}, {"value", "bar"}}}},
        }},
    }));

    auto response = handler->router().handle(request);
    ASSERT_EQ(response.body().toStdString(), "{}");
    ASSERT_EQ(response.status(), HTTP_OK);

    // ASSERT_TRUE(index->containsDocument(111));
    // ASSERT_TRUE(index->containsDocument(112));
    ASSERT_EQ(index->info().attribute("foo").toStdString(), "bar");
}
