#ifndef ACOUSTID_INDEX_SERVER_HTTP_REQUEST_H_
#define ACOUSTID_INDEX_SERVER_HTTP_REQUEST_H_

#include <QJsonDocument>
#include <QMap>
#include <QString>
#include <QUrl>
#include <QUrlQuery>
#include <qhttpserverrequest.hpp>

namespace Acoustid {
namespace Server {

typedef qhttp::THttpMethod HttpMethod;

const auto HTTP_GET = qhttp::EHTTP_GET;
const auto HTTP_POST = qhttp::EHTTP_POST;
const auto HTTP_PUT = qhttp::EHTTP_PUT;
const auto HTTP_DELETE = qhttp::EHTTP_DELETE;
const auto HTTP_HEAD = qhttp::EHTTP_HEAD;
const auto HTTP_OPTIONS = qhttp::EHTTP_OPTIONS;

typedef qhttp::THeaderHash HttpHeaderMap;

class HttpRequest {
 public:
    HttpRequest(HttpMethod method, const QUrl &url = QUrl());
    HttpRequest(const HttpRequest &other, const QMap<QString, QString> &args);

    HttpMethod method() const { return m_method; }

    QString param(const QString &name, const QString &defaultValue = QString()) const;

    QUrl url() const;

    QJsonDocument json() const;

    QByteArray body() const { return m_body; }

    void setMethod(HttpMethod method) { m_method = method; }

    void setUrl(const QUrl &url) {
        m_url = url;
        m_query = QUrlQuery(url);
    }

    void setHeaders(const qhttp::THeaderHash &headers) { m_headers = headers; }
    void setHeader(const QString &name, const QString &value) { m_headers.insert(name.toUtf8(), value.toUtf8()); }

    void setBody(const QByteArray &data) { m_body = data; }
    void setBody(const QJsonDocument &doc);

    void setArgs(const QMap<QString, QString> &args) { m_args = args; }

 private:
    HttpMethod m_method;
    QUrl m_url;
    QUrlQuery m_query;
    HttpHeaderMap m_headers;
    QByteArray m_body;
    QMap<QString, QString> m_args;
};

}  // namespace Server
}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_SERVER_HTTP_REQUEST_H_
