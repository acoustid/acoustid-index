#ifndef ACOUSTID_INDEX_SERVER_HTTP_ROUTER_H_
#define ACOUSTID_INDEX_SERVER_HTTP_ROUTER_H_

#include <QList>
#include <QPair>
#include <QRegularExpression>

#include "request.h"
#include "response.h"

namespace Acoustid {
namespace Server {

typedef std::function<HttpResponse(const HttpRequest &)> HttpHandlerFunc;

class HttpRouter {
 public:
    void route(HttpMethod method, const QString &path, HttpHandlerFunc handler);

    HttpResponse handle(const HttpRequest &request) const;
    void handle(qhttp::server::QHttpRequest *request, qhttp::server::QHttpResponse *response) const;

 private:
    QMap<HttpMethod, QList<QPair<QRegularExpression, HttpHandlerFunc>>> m_routes;
};

}  // namespace Server
}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_SERVER_HTTP_ROUTER_H_
