#ifndef ACOUSTID_INDEX_SERVER_HTTP_RESPONSE_H_
#define ACOUSTID_INDEX_SERVER_HTTP_RESPONSE_H_

#include <QJsonDocument>

#include "request.h"

namespace Acoustid {
namespace Server {

typedef qhttp::TStatusCode HttpStatusCode;

const auto HTTP_OK = qhttp::ESTATUS_OK;
const auto HTTP_NOT_FOUND = qhttp::ESTATUS_NOT_FOUND;
const auto HTTP_BAD_REQUEST = qhttp::ESTATUS_BAD_REQUEST;
const auto HTTP_INTERNAL_SERVER_ERROR = qhttp::ESTATUS_INTERNAL_SERVER_ERROR;
const auto HTTP_SERVICE_UNAVAILABLE = qhttp::ESTATUS_SERVICE_UNAVAILABLE;

class HttpResponse {
 public:
    HttpResponse() : m_status(HTTP_OK) {}
    HttpResponse(HttpStatusCode status) : m_status(status) {}
    HttpResponse(HttpStatusCode status, const QString &text) : m_status(status) { setBody(text); }
    HttpResponse(HttpStatusCode status, const QJsonDocument &doc) : m_status(status) { setBody(doc); }

    HttpStatusCode status() const { return m_status; }
    void setStatus(HttpStatusCode status);

    QString header(const QString &name) const { return m_headers[name]; }
    void setHeader(const QString &name, const QString &value);

    const QByteArray &body() const { return m_body; }
    void setBody(const QString &text);
    void setBody(const QJsonDocument &doc);

    void send(qhttp::server::QHttpRequest *req, qhttp::server::QHttpResponse *res) const;

 private:
    HttpStatusCode m_status;
    QMap<QString, QString> m_headers;
    QByteArray m_body;
};

class HttpResponseException : public std::exception {
 public:
    HttpResponseException(const HttpResponse &response);

    const HttpResponse &response() const { return m_response; }

    const char *what() const noexcept override { return m_what.c_str(); }

 private:
    HttpResponse m_response;
    std::string m_what;
};

}  // namespace Server
}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_SERVER_HTTP_RESPONSE_H_
