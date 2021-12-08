// Copyright (C) 2019  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SERVER_HTTP_H_
#define ACOUSTID_INDEX_SERVER_HTTP_H_

#include <QSharedPointer>

#include "server/http/request.h"
#include "server/http/response.h"
#include "server/http/router.h"

namespace Acoustid {

class Index;

namespace Server {

class Metrics;

class HttpRequestHandler : public QObject {
    Q_OBJECT

 public:
    HttpRequestHandler(QSharedPointer<Index> indexes, QSharedPointer<Metrics> metrics);

    const HttpRouter &router() const { return m_router; }

 private:
    QSharedPointer<Index> m_indexes;
    QSharedPointer<Metrics> m_metrics;

    HttpRouter m_router;

    void sendResponse(const HttpResponse &response, qhttp::server::QHttpRequest *req,
                      qhttp::server::QHttpResponse *res);
};

}  // namespace Server
}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_SERVER_HTTP_H_
