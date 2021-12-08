#include "router.h"

#include <QRegularExpression>
#include <QtConcurrent>

namespace Acoustid {
namespace Server {

void HttpRouter::route(HttpMethod method, const QString &path, HttpHandlerFunc handler) {
    auto pathParts = path.split('/');
    for (auto &pathPart : pathParts) {
        if (pathPart.startsWith(':')) {
            auto paramName = pathPart.midRef(1);
            pathPart = "(?<" + paramName + ">[^/]+)";
        }
    }
    m_routes[method].append({QRegularExpression("^" + pathParts.join("/") + "$"), handler});
}

HttpResponse HttpRouter::handle(const HttpRequest &request) const {
    auto it = m_routes.find(request.method());
    if (it == m_routes.end()) {
        return HttpResponse(HTTP_NOT_FOUND);
    }

    const auto path = request.url().path();
    for (const auto &route : *it) {
        const auto pattern = route.first;
        const auto match = pattern.match(path);
        if (match.hasMatch()) {
            QMap<QString, QString> args;
            auto argNames = pattern.namedCaptureGroups();
            for (auto i = 1; i < argNames.size(); ++i) {
                const auto argName = QString(":") + argNames[i];
                args[argName] = match.captured(i);
            }
            try {
                return route.second(HttpRequest(request, args));
            } catch (HttpResponseException &e) {
                return e.response();
            }
        }
    }

    return HttpResponse(HTTP_NOT_FOUND);
}

void HttpRouter::handle(qhttp::server::QHttpRequest *req, qhttp::server::QHttpResponse *res) const {
    req->collectData();
    req->onEnd([=]() {
        HttpRequest request(req->method(), req->url());
        request.setHeaders(req->headers());
        request.setBody(req->collectedData());
        QtConcurrent::run([=]() {
            HttpResponse response;
            try {
                response = handle(request);
            } catch (const std::exception &e) {
                qDebug() << "Error handling request:" << e.what();
                response = HttpResponse(HTTP_INTERNAL_SERVER_ERROR);
            }
            QMetaObject::invokeMethod(req, [=]() {
                response.send(req, res);
            });
        });
    });
}

}  // namespace Server
}  // namespace Acoustid
