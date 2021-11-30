#include "http.h"

#include "metrics.h"

using namespace qhttp::server;

namespace Acoustid {
namespace Server {

static void sendContent(QHttpResponse *res, const QString &content) {
    auto contentBytes = content.toUtf8();
    res->addHeaderValue("Content-Length", contentBytes.size());
    res->end(contentBytes);
}

void handleHttpRequest(QHttpRequest *req, QHttpResponse *res, QSharedPointer<Metrics> metrics) {
    auto url = req->url();
    if (url.path() == "/metrics") {
        auto content = metrics->toStringList().join("\n") + "\n";
        res->setStatusCode(qhttp::ESTATUS_OK);
        sendContent(res, content);
    } else if (url.path() == "/health/ready") {
        res->setStatusCode(qhttp::ESTATUS_OK);
        res->addHeader("Content-Type", "text/plain; charset=utf-8");
        sendContent(res, "OK\n");
    } else if (url.path() == "/health/alive") {
        res->setStatusCode(qhttp::ESTATUS_OK);
        res->addHeader("Content-Type", "text/plain; charset=utf-8");
        sendContent(res, "OK\n");
    } else {
        res->setStatusCode(qhttp::ESTATUS_NOT_FOUND);
        sendContent(res, "");
    }
}

}  // namespace Server
}  // namespace Acoustid
