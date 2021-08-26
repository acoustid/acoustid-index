// Copyright (C) 2020  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_PROTOCOL_H_
#define ACOUSTID_SERVER_PROTOCOL_H_

#include <functional>
#include <QString>
#include <QStringList>
#include <QSharedPointer>

#include "request.h"

namespace Acoustid { namespace Server {

class Session;

typedef std::function<QString(QSharedPointer<Session> session)> ScopedHandlerFunc;
typedef std::function<QString()> HandlerFunc;

QString renderResponse(const QString &response);
QString renderErrorResponse(const QString &response);

QSharedPointer<Request> parseRequest(const QString &line);

ScopedHandlerFunc buildHandler(const QString &command, const QStringList &args);
HandlerFunc injectSessionIntoHandler(QWeakPointer<Session> session, ScopedHandlerFunc handler);

} // namespace Server
} // namespace Acoustid

#endif
