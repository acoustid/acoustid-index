#include "http.h"

#include <QtConcurrent>

#include "index/multi_index.h"
#include "metrics.h"

using namespace qhttp;
using namespace qhttp::server;

namespace Acoustid {
namespace Server {

static void sendStringResponse(qhttp::TStatusCode status, QHttpResponse *res, const QString &content) {
    qDebug() << "Sending response" << status << content << "from thread" << QThread::currentThreadId();
    auto contentBytes = content.toUtf8();
    res->setStatusCode(status);
    res->addHeaderValue("Content-Length", contentBytes.size());
    res->end(contentBytes);
}

HttpRequest::HttpRequest(qhttp::server::QHttpRequest *req, const QMap<QString, QString> &args)
    : m_req(req), m_args(args) {}

QString HttpRequest::arg(const QString &name, const QString &defaultValue) const {
    auto it = m_args.find(name);
    if (it == m_args.end()) {
        return defaultValue;
    }
    return *it;
}

QUrl HttpRequest::url() const { return m_req->url(); }

HttpResponse::HttpResponse() {}

void HttpResponse::setStatus(qhttp::TStatusCode status) { m_status = status; }

void HttpResponse::setHeader(const QString &key, const QString &value) { m_headers[key] = value; }

void HttpResponse::setBody(const QString &text) {
    m_body = text.toUtf8();
    setHeader("Content-Type", "text/plain; charset=utf-8");
}

void HttpResponse::setBody(const QJsonDocument &doc) {
    m_body = doc.toJson();
    setHeader("Content-Type", "application/json; charset=utf-8");
}

void HttpResponse::send(qhttp::server::QHttpResponse *res) const {
    res->setStatusCode(m_status);
    res->addHeaderValue("Content-Length", m_body.size());
    for (auto it = m_headers.begin(); it != m_headers.end(); ++it) {
        res->addHeaderValue(it.key().toUtf8(), it.value());
    }
    res->end(m_body);
}

HttpRequestHandler::HttpRequestHandler(QSharedPointer<MultiIndex> indexes, QSharedPointer<Metrics> metrics)
    : m_indexes(indexes), m_metrics(metrics) {
    // Healthchecks
    addHandler(qhttp::EHTTP_GET, "/_health/alive",
               [=](const HttpRequest &req) { return makeResponse(qhttp::ESTATUS_OK, "OK\n"); });
    addHandler(qhttp::EHTTP_GET, "/_health/ready",
               [=](const HttpRequest &req) { return makeResponse(qhttp::ESTATUS_OK, "OK\n"); });

    // Prometheus metrics
    addHandler(qhttp::EHTTP_GET, "/_metrics", [=](const HttpRequest &req) {
        auto content = m_metrics->toStringList().join("\n") + "\n";
        return makeResponse(qhttp::ESTATUS_OK, content);
    });

    // Check if index exists
    const QString indexPatternPrefix = "/(?<index>[^_/][^/]*)";
    addHandler(qhttp::EHTTP_HEAD, indexPatternPrefix, [=](const HttpRequest &req) {
        auto indexName = req.arg("index");
        if (!m_indexes->indexExists(indexName)) {
            return makeResponse(qhttp::ESTATUS_NOT_FOUND, QJsonDocument());
        }
        return makeResponse(qhttp::ESTATUS_OK, QJsonDocument());
    });

    // Get index info
    addHandler(qhttp::EHTTP_GET, indexPatternPrefix, [=](const HttpRequest &req) {
        auto indexName = req.arg("index");
        try {
            auto index = m_indexes->getIndex(indexName);
            QJsonObject result{
                {"name", indexName},
                {"revision", index->info().revision()},
            };
            return makeResponse(qhttp::ESTATUS_OK, QJsonDocument(result));
        } catch (const IndexNotFoundException &) {
            return makeResponse(qhttp::ESTATUS_NOT_FOUND, QJsonDocument(QJsonObject()));
        }
    });

    // Create index
    addHandler(qhttp::EHTTP_PUT, indexPatternPrefix, [=](const HttpRequest &req) {
        auto indexName = req.arg("index");
        auto index = m_indexes->getIndex(indexName, true);
        QJsonObject result{
            {"name", indexName},
            {"revision", index->info().revision()},
        };
        return makeResponse(qhttp::ESTATUS_OK, QJsonDocument(result));
    });

    // Delete index
    addHandler(qhttp::EHTTP_DELETE, indexPatternPrefix, [=](const HttpRequest &req) {
        auto indexName = req.arg("index");
        m_indexes->deleteIndex(indexName);
        return makeResponse(qhttp::ESTATUS_OK, QJsonDocument(QJsonObject()));
    });

    // Search
    addHandler(qhttp::EHTTP_GET, indexPatternPrefix + "/_search", [=](const HttpRequest &req) {
        auto indexName = req.arg("index");
        try {
            auto index = m_indexes->getIndex(indexName);
            auto results = index->search({1, 2, 3});
            QJsonArray resultsJson;
            for (auto &result : results) {
                resultsJson.append(QJsonObject{
                    {"id", qint64(result.docId())},
                    {"score", result.score()},
                });
            }
            QJsonObject responseJson{
                {"results", resultsJson},
            };
            return makeResponse(qhttp::ESTATUS_OK, QJsonDocument(responseJson));
        } catch (const IndexNotFoundException &) {
            QJsonObject responseJson{
                {"error",
                 QJsonObject{
                     {"type", "index_not_found"},
                 }},
                {"status", int(qhttp::ESTATUS_NOT_FOUND)},
            };
            return makeResponse(qhttp::ESTATUS_NOT_FOUND, QJsonDocument(responseJson));
        }
    });

    /*


        // Index API
        const QString indexPatternPrefix = "/(?<index>[^_/][^/]*)";
        addHandler(qhttp::EHTTP_HEAD, indexPatternPrefix,
                   [this](QHttpRequest *req, QHttpResponse *res, const QMap<QString, QString> &args) {
                       const auto indexName = args["index"];
                       qDebug() << "Checking status of index" << indexName;
                       sendStringResponse(qhttp::ESTATUS_OK, res, "ok\n");
                   });
        addHandler(qhttp::EHTTP_GET, indexPatternPrefix,
                   [this](QHttpRequest *req, QHttpResponse *res, const QMap<QString, QString> &args) {
                       const auto indexName = args["index"];
                       qDebug() << "Getting index" << indexName;
                       sendStringResponse(qhttp::ESTATUS_OK, res, "ok\n");
                   });
        addHandler(qhttp::EHTTP_PUT, indexPatternPrefix,
                   [this](QHttpRequest *req, QHttpResponse *res, const QMap<QString, QString> &args) {
                       const auto indexName = args["index"];
                       qDebug() << "Creating index" << indexName;
                       sendStringResponse(qhttp::ESTATUS_OK, res, "ok\n");
                   });
        addHandler(qhttp::EHTTP_DELETE, indexPatternPrefix,
                   [this](QHttpRequest *req, QHttpResponse *res, const QMap<QString, QString> &args) {
                       const auto indexName = args["index"];
                       qDebug() << "Deleting index" << indexName;
                       sendStringResponse(qhttp::ESTATUS_OK, res, "ok\n");
                   });

        // Document API
        const QString docIdPatternPrefix = indexPatternPrefix + "/(?<id>[0-9]+)";
        addHandler(qhttp::EHTTP_POST, indexPatternPrefix,
                   [this](QHttpRequest *req, QHttpResponse *res, const QMap<QString, QString> &args) {
                       const auto indexName = args["index"];
                       qDebug() << "Adding fingerprints to index" << indexName;
                       sendStringResponse(qhttp::ESTATUS_OK, res, "ok\n");
                   });
        addHandler(qhttp::EHTTP_GET, docIdPatternPrefix,
                   [this](QHttpRequest *req, QHttpResponse *res, const QMap<QString, QString> &args) {
                       const auto indexName = args["index"];
                       const auto docId = args["id"];
                       qDebug() << "Getting fingerprint" << docId << "from index" << indexName;
                       sendStringResponse(qhttp::ESTATUS_OK, res, "ok\n");
                   });
        addHandler(qhttp::EHTTP_PUT, docIdPatternPrefix,
                   [this](QHttpRequest *req, QHttpResponse *res, const QMap<QString, QString> &args) {
                       const auto indexName = args["index"];
                       const auto docId = args["id"];
                       qDebug() << "Updating fingerprint" << docId << "in index" << indexName;
                       sendStringResponse(qhttp::ESTATUS_OK, res, "ok\n");
                   });
        addHandler(qhttp::EHTTP_DELETE, docIdPatternPrefix,
                   [this](QHttpRequest *req, QHttpResponse *res, const QMap<QString, QString> &args) {
                       const auto indexName = args["index"];
                       const auto docId = args["id"];
                       qDebug() << "Deleting fingerprint" << docId << "in index" << indexName;
                       sendStringResponse(qhttp::ESTATUS_OK, res, "ok\n");
                   });

        // Search API
        addHandler(qhttp::EHTTP_GET, indexPatternPrefix + "/_search",
                   [this](QHttpRequest *req, QHttpResponse *res, const QMap<QString, QString> &args) {
                       qDebug() << "Received search in thread" << QThread::currentThreadId();
                       const auto indexName = args["index"];
                       qDebug() << "Searching in index" << indexName;
                       QtConcurrent::run([this, indexName, req, res]() {
                           qDebug() << "Running search in thread" << QThread::currentThreadId();
                           //                       auto index = m_indexes->getIndex(indexName);
                           //                       auto results = index->search({1, 2, 3});
                           QJsonObject response;
                           response["status"] = "ok";
                           QJsonDocument doc(response);
                           QMetaObject::invokeMethod(this,
                                                     [=]() { sendStringResponse(qhttp::ESTATUS_OK, res, doc.toJson());
       });
                       });
                   });*/
}

void HttpRequestHandler::addHandler(qhttp::THttpMethod method, const QString &pattern, HttpRequestHandlerFunc handler) {
    m_handlers.push_back({method, QRegularExpression("^" + pattern + "$"), handler});
}

void HttpRequestHandler::sendResponse(const HttpResponse &response, qhttp::server::QHttpRequest *req,
                                      qhttp::server::QHttpResponse *res) {
    qDebug().nospace() << "method=" << qhttp::Stringify::toString(req->method()) << " path=" << req->url().path()
                       << " status=" << response.status();
    response.send(res);
}

void HttpRequestHandler::handleRequest(QHttpRequest *req, QHttpResponse *res) {
    for (auto it = m_handlers.begin(); it != m_handlers.end(); ++it) {
        const auto method = std::get<0>(*it);
        const auto pattern = std::get<1>(*it);
        const auto handler = std::get<2>(*it);
        if (method == req->method()) {
            const auto match = pattern.match(req->url().path());
            if (match.hasMatch()) {
                QMap<QString, QString> args;
                auto argNames = pattern.namedCaptureGroups();
                for (auto i = 1; i < argNames.size(); ++i) {
                    args[argNames[i]] = match.captured(i);
                }
                req->collectData();
                req->onEnd([=]() {
                    QtConcurrent::run([=]() {
                        HttpResponse response;
                        try {
                            response = handler(HttpRequest(req, args));
                        } catch (std::exception &e) {
                            qDebug() << "Error handling request:" << e.what();
                            response = makeResponse(qhttp::ESTATUS_INTERNAL_SERVER_ERROR, "internal server error\n");
                        }
                        QMetaObject::invokeMethod(this, [=]() { sendResponse(response, req, res); });
                    });
                });
                return;
            }
        }
    }
    sendResponse(makeResponse(qhttp::ESTATUS_NOT_FOUND, "not found\n"), req, res);
}

}  // namespace Server
}  // namespace Acoustid
