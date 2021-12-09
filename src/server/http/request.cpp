#include "request.h"

namespace Acoustid {
namespace Server {

HttpRequest::HttpRequest(HttpMethod method, const QUrl &url) { setMethod(method), setUrl(url); }

HttpRequest::HttpRequest(const HttpRequest &other, const QMap<QString, QString> &args) {
    setMethod(other.method());
    setUrl(other.url());
    setHeaders(other.m_headers);
    setBody(other.m_body);
    setArgs(args);
}

QString HttpRequest::param(const QString &name, const QString &defaultValue) const {
    auto it = m_args.find(name);
    if (it != m_args.end()) {
        return *it;
    }
    if (m_query.hasQueryItem(name)) {
        return m_query.queryItemValue(name);
    }
    return defaultValue;
}

QUrl HttpRequest::url() const { return m_url; }

QJsonDocument HttpRequest::json() const {
    QJsonParseError error;
    auto doc = QJsonDocument::fromJson(m_body, &error);
    return doc;
}

void HttpRequest::setBody(const QJsonDocument &body) {
    m_body = body.toJson(QJsonDocument::Compact);
    m_headers.insert("Content-Type", "application/json");
}

}  // namespace Server
}  // namespace Acoustid
