// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QThreadPool>
#include <QtConcurrent>
#include "listener.h"
#include "connection.h"
#include "session.h"
#include "errors.h"
#include "protocol.h"

using namespace Acoustid;
using namespace Acoustid::Server;

static const char* kCRLF = "\r\n";
static const int kMaxLineSize = 1024 * 32;

Connection::Connection(IndexSharedPtr index, QTcpSocket *socket, QObject *parent)
    : QObject(parent), m_socket(socket), m_stream(socket), m_handler(new QFutureWatcher<QString>(this))
{
    m_socket->setParent(this);
    m_client = QString("%1:%2").arg(m_socket->peerAddress().toString()).arg(m_socket->peerPort());

    m_session = QSharedPointer<Session>(new Session(index, listener()->metrics()));

    connect(m_socket, &QTcpSocket::readyRead, this, &Connection::readIncomingData);
    connect(m_socket, &QTcpSocket::disconnected, this, &Connection::disconnected);

    connect(m_handler, &QFutureWatcher<QString>::resultReadyAt, [this](int index) {
        auto result = m_handler->resultAt(index);
        sendResponse(result);
    });
}

Connection::~Connection()
{
}

Listener *Connection::listener() const
{
    return qobject_cast<Listener *>(parent());
}

void Connection::close()
{
    m_socket->disconnectFromHost();
}

void Connection::sendResponse(const QString& response, bool next)
{
    m_stream << response << kCRLF << flush;
    if (next) {
        readIncomingData();
    }
}

void Connection::readIncomingData()
{
    auto log_prefix = "[" + m_client + "]";

    if (m_handler->isRunning()) {
        qWarning() << log_prefix << "Got data while still handling the previous command, closing connection";
        close();
        return;
    }

    if (m_stream.readLineInto(&m_line, kMaxLineSize)) {
        if (m_line.size() >= kMaxLineSize) {
            qWarning() << log_prefix << "Got too long request, closing connection";
            sendResponse(renderErrorResponse("line too long"), false);
            close();
            return;
        }

        HandlerFunc handler;
        try {
            handler = injectSessionIntoHandler(m_session, buildHandler(m_line));
        }
        catch (const CloseRequested &ex) {
            qDebug() << log_prefix << "Client request to close the connection";
            sendResponse(renderResponse(""), false);
            close();
            return;
        }
        catch (const ProtocolException &ex) {
            qInfo() << log_prefix << "Protocol error:" << ex.what();
            sendResponse(renderErrorResponse(ex.what()), false);
            return;
        }
        catch (const Exception &ex) {
            qCritical() << log_prefix << "Unexpected exception while building handler:" << ex.what();
            sendResponse(renderErrorResponse(ex.what()), false);
            return;
        }

        auto result = QtConcurrent::run([=]() {
            try {
                return renderResponse(handler());
            }
            catch (const HandlerException &ex) {
                qInfo() << log_prefix << "Handler error:" << ex.what();
                return renderErrorResponse(ex.what());
            }
            catch (const Exception &ex) {
                qCritical() << log_prefix << "Unexpected exception while running handler:" << ex.what();
                return renderErrorResponse(ex.what());
            }
        });
        m_handler->setFuture(result);
    }

}

