#include "response.h"

#include <qhttpserverresponse.hpp>

namespace Acoustid {
namespace Server {

void HttpResponse::setStatus(qhttp::TStatusCode status) { m_status = status; }

void HttpResponse::setHeader(const QString &key, const QString &value) { m_headers[key] = value; }

void HttpResponse::setBody(const QString &text) {
    m_body = text.toUtf8();
    setHeader("Content-Type", "text/plain; charset=utf-8");
}

void HttpResponse::setBody(const QJsonDocument &doc) {
    m_body = doc.toJson(QJsonDocument::Compact);
    setHeader("Content-Type", "application/json; charset=utf-8");
}

void HttpResponse::send(qhttp::server::QHttpRequest *req, qhttp::server::QHttpResponse *res) const {
    res->setStatusCode(m_status);
    for (auto it = m_headers.begin(); it != m_headers.end(); ++it) {
        res->addHeaderValue(it.key().toUtf8(), it.value());
    }
    if (req->method() == qhttp::EHTTP_HEAD) {
        res->end();
        return;
    }
    res->addHeaderValue("Content-Length", m_body.size());
    res->end(m_body);
}

HttpResponseException::HttpResponseException(const HttpResponse &response) : m_response(response) {
    m_what = QString("HTTP %1").arg(m_response.status()).toStdString();
}

}  // namespace Server
}  // namespace Acoustid
