// Copyright (C) 2019  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_HTTP_H_
#define ACOUSTID_SERVER_HTTP_H_

#include <QSharedPointer>

#include "qhttpserver.hpp"
#include "qhttpserverrequest.hpp"
#include "qhttpserverresponse.hpp"

namespace Acoustid {
namespace Server {

class Metrics;

void handleHttpRequest(qhttp::server::QHttpRequest *req, qhttp::server::QHttpResponse *res, QSharedPointer<Metrics> metrics);

}  // namespace Server
}  // namespace Acoustid

#endif
