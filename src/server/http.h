// Copyright (C) 2019  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_HTTP_H_
#define ACOUSTID_SERVER_HTTP_H_

#include <QRegularExpression>
#include <QSharedPointer>

#include "qhttpserver.hpp"
#include "qhttpserverrequest.hpp"
#include "qhttpserverresponse.hpp"

namespace Acoustid {

class Index;

namespace Server {

class Metrics;

class HttpRequestHandler : public QObject {
    Q_OBJECT

 public:
    HttpRequestHandler(QSharedPointer<Index> index, QSharedPointer<Metrics> metrics);

    void handleRequest(qhttp::server::QHttpRequest *req, qhttp::server::QHttpResponse *res);

 private:
    QSharedPointer<Index> m_index;
    QSharedPointer<Metrics> m_metrics;

    typedef std::function<void(qhttp::server::QHttpRequest *, qhttp::server::QHttpResponse *)> Handler;
    typedef std::function<void(qhttp::server::QHttpRequest *, qhttp::server::QHttpResponse *,
                               QMap<QString, QString> args)>
        HandlerWithArgs;

    void addHandler(qhttp::THttpMethod, const QString &pattern, Handler handler);
    void addHandler(qhttp::THttpMethod, const QString &pattern, HandlerWithArgs handler);

    std::vector<std::tuple<qhttp::THttpMethod, QRegularExpression, HandlerWithArgs>> m_handlers;
};

}  // namespace Server
}  // namespace Acoustid

#endif
