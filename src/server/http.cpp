#include "http.h"

#include "metrics.h"

using namespace qhttp;
using namespace qhttp::server;

namespace Acoustid {
namespace Server {

static void sendStringResponse(qhttp::TStatusCode status, QHttpResponse *res, const QString &content) {
    auto contentBytes = content.toUtf8();
    res->setStatusCode(status);
    res->addHeaderValue("Content-Length", contentBytes.size());
    res->end(contentBytes);
}

HttpRequestHandler::HttpRequestHandler(QSharedPointer<Index> index, QSharedPointer<Metrics> metrics)
    : m_index(index), m_metrics(metrics) {
    // Healthchecks
    addHandler(qhttp::EHTTP_GET, "/_health/alive",
               [this](QHttpRequest *req, QHttpResponse *res) { sendStringResponse(qhttp::ESTATUS_OK, res, "ok\n"); });
    addHandler(qhttp::EHTTP_GET, "/_health/ready",
               [this](QHttpRequest *req, QHttpResponse *res) { sendStringResponse(qhttp::ESTATUS_OK, res, "ok\n"); });

    // Prometheus metrics
    addHandler(qhttp::EHTTP_GET, "/_metrics", [this](QHttpRequest *req, QHttpResponse *res) {
        auto content = m_metrics->toStringList().join("\n") + "\n";
        sendStringResponse(qhttp::ESTATUS_OK, res, content);
    });

    // Index API
    const auto indexPatternPrefix = "/(?<index>[^_/][^/]*)";
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
}

void HttpRequestHandler::addHandler(qhttp::THttpMethod method, const QString &pattern, Handler handler) {
    m_handlers.push_back(
        {method, QRegularExpression(pattern),
         [=](QHttpRequest *req, QHttpResponse *res, QMap<QString, QString> args) { handler(req, res); }});
}

void HttpRequestHandler::addHandler(qhttp::THttpMethod method, const QString &pattern, HandlerWithArgs handler) {
    m_handlers.push_back({method, QRegularExpression(pattern), handler});
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
                handler(req, res, args);
                return;
            }
        }
    }
    sendStringResponse(qhttp::ESTATUS_NOT_FOUND, res, "not found\n");
}

}  // namespace Server
}  // namespace Acoustid
