// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "connection.h"

#include <QThreadPool>
#include <QTimer>
#include <QtConcurrent>

#include "errors.h"
#include "listener.h"
#include "metrics.h"
#include "protocol.h"
#include "session.h"

using namespace Acoustid;
using namespace Acoustid::Server;

static const char *kCRLF = "\r\n";
static const int kMaxLineSize = 1024 * 32;

Connection::Connection(IndexSharedPtr index, QTcpSocket *socket, QObject *parent)
    : QObject(parent),
      m_socket(socket),
      m_stream(socket),
      m_handler(new QFutureWatcher<QPair<QSharedPointer<Request>, QString>>(this)),
      m_idle_timeout_timer(new QTimer(this)) {
    m_socket->setParent(this);
    m_client = QString("%1:%2").arg(m_socket->peerAddress().toString()).arg(m_socket->peerPort());

    m_session = QSharedPointer<Session>(new Session(index, listener()->metrics()));

    connect(m_socket, &QTcpSocket::readyRead, this, &Connection::readIncomingData);
    connect(m_socket, &QTcpSocket::disconnected, this, &Connection::disconnected);

    connect(m_handler, &QFutureWatcher<QPair<QSharedPointer<Request>, QString>>::finished, [this]() {
        if (!m_handler->isCanceled()) {
            auto result = m_handler->result();
            sendResponse(result.first, result.second);
        }
        QTimer::singleShot(0, this, &Connection::readIncomingData);
    });

    connect(m_idle_timeout_timer, &QTimer::timeout, this, [this]() {
        auto log_prefix = "[" + m_client + "]";
        qDebug() << log_prefix << "Idle for" << (m_idle_timeout_timer->interval() / 1000.0) << "seconds, closing connection";
        close();
    });

    resetIdleTimeoutTimer();
}

Connection::~Connection() {}

void Connection::resetIdleTimeoutTimer() {
    m_idle_timeout_timer->setSingleShot(true);
    m_idle_timeout_timer->start(m_session->getIdleTimeout());
}

Listener *Connection::listener() const { return qobject_cast<Listener *>(parent()); }

void Connection::close() {
    if (!m_handler->isFinished()) {
        m_handler->cancel();
    }
    m_socket->disconnectFromHost();
}

void Connection::sendResponse(const QSharedPointer<Request> &request, const QString &response) {
    resetIdleTimeoutTimer();
    m_stream << response << kCRLF << flush;
    if (request) {
        m_session->metrics()->onRequest(request->command(), request->elapsed());
    } else {
        m_session->metrics()->onRequest("unknown", 0);
    }
}

void Connection::readIncomingData() {
    resetIdleTimeoutTimer();

    auto log_prefix = "[" + m_client + "]";

    if (m_stream.readLineInto(&m_line, kMaxLineSize)) {
        if (m_line.size() >= kMaxLineSize) {
            qDebug() << log_prefix << "Received request that is too long, closing connection";
            sendResponse(nullptr, renderErrorResponse("line too long"));
            close();
            return;
        }

        if (!m_handler->isFinished()) {
            qWarning() << log_prefix
                       << "Received request while still handling the previous "
                          "one, closing connection";
            sendResponse(nullptr, renderErrorResponse("previous request is still in progress"));
            close();
            return;
        }

        QSharedPointer<Request> request;
        HandlerFunc handler;

        try {
            request = parseRequest(m_line);
        } catch (const ProtocolException &ex) {
            sendResponse(request, renderErrorResponse(ex.what()));
            return;
        }

        if (request->command() == "quit") {
            sendResponse(request, renderResponse(""));
            close();
            return;
        }

        try {
            handler = injectSessionIntoHandler(m_session, buildHandler(request->command(), request->args()));
        } catch (const ProtocolException &ex) {
            sendResponse(request, renderErrorResponse(ex.what()));
            return;
        }

        auto futureResult = QtConcurrent::run([=]() {
            QString response;
            try {
                response = renderResponse(handler());
            } catch (const HandlerException &ex) {
                qInfo() << log_prefix << "Handler error:" << ex.what();
                response = renderErrorResponse(ex.what());
            } catch (const Exception &ex) {
                qCritical() << log_prefix << "Unexpected exception:" << ex.what();
                response = renderErrorResponse("internal error");
            }
            return qMakePair(request, response);
        });

        m_handler->setFuture(futureResult);
    }
}
