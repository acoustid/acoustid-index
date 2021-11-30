// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "listener.h"

#include <signal.h>
#include <sys/socket.h>
#include <syslog.h>

#include <QCoreApplication>

#include "connection.h"
#include "metrics.h"
#include "store/fs_directory.h"

using namespace Acoustid;
using namespace Acoustid::Server;

int Listener::m_sigIntFd[2];
int Listener::m_sigTermFd[2];

Listener::Listener(const QString& path, bool mmap, QObject* parent)
    : QTcpServer(parent), m_dir(new FSDirectory(path, mmap)), m_index(new Index(m_dir, true)), m_metrics(new Metrics()) {
    m_sigIntNotifier = new QSocketNotifier(m_sigIntFd[1], QSocketNotifier::Read, this);
    connect(m_sigIntNotifier, &QSocketNotifier::activated, this, &Listener::handleSigInt);
    m_sigTermNotifier = new QSocketNotifier(m_sigTermFd[1], QSocketNotifier::Read, this);
    connect(m_sigTermNotifier, &QSocketNotifier::activated, this, &Listener::handleSigTerm);
    connect(this, &QTcpServer::newConnection, this, &Listener::acceptNewConnection);
}

Listener::~Listener() {}

void Listener::sigIntHandler(int signal) {
    char tmp = 1;
    ::write(m_sigIntFd[0], &tmp, sizeof(tmp));
}

void Listener::sigTermHandler(int signal) {
    char tmp = 1;
    ::write(m_sigTermFd[0], &tmp, sizeof(tmp));
}

void Listener::setupSignalHandlers() {
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, m_sigIntFd)) {
        qFatal("Couldn't create SIGINT socketpair");
    }
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, m_sigTermFd)) {
        qFatal("Couldn't create SIGTERM socketpair");
    }
    struct sigaction sigint;
    sigint.sa_handler = Listener::sigIntHandler;
    sigemptyset(&sigint.sa_mask);
    sigint.sa_flags = 0;
    sigint.sa_flags |= SA_RESTART;
    if (sigaction(SIGINT, &sigint, 0) > 0) {
        qFatal("Couldn't install SIGINT handler");
    }
    struct sigaction sigterm;
    sigterm.sa_handler = Listener::sigTermHandler;
    sigemptyset(&sigterm.sa_mask);
    sigterm.sa_flags = 0;
    sigterm.sa_flags |= SA_RESTART;
    if (sigaction(SIGTERM, &sigterm, 0) > 0) {
        qFatal("Couldn't install SIGTERM handler");
    }
}

void Listener::handleSigInt() {
    m_sigIntNotifier->setEnabled(false);
    char tmp;
    ::read(m_sigIntFd[1], &tmp, sizeof(tmp));
    qDebug() << "Received SIGINT, stopping";
    stop();  // XXX handle this more gracefully
    m_sigIntNotifier->setEnabled(true);
}

void Listener::handleSigTerm() {
    m_sigTermNotifier->setEnabled(false);
    char tmp;
    ::read(m_sigTermFd[1], &tmp, sizeof(tmp));
    qDebug() << "Received SIGTERM, stopping";
    stop();
    m_sigTermNotifier->setEnabled(true);
}

void Listener::stop() {
    qDebug() << "Stopping the listener";
    close();
    if (m_connections.isEmpty()) {
        qApp->quit();
    } else {
        connect(this, &Listener::lastConnectionClosed, qApp, &QCoreApplication::quit);
        foreach (Connection* connection, m_connections) { connection->close(); }
    }
}

void Listener::removeConnection(Connection* connection) {
    qDebug() << "Disconnected from" << connection->client();
    metrics()->onClosedConnection();
    connection->deleteLater();
    m_connections.removeAll(connection);
    if (m_connections.isEmpty()) {
        emit lastConnectionClosed();
    }
}

void Listener::acceptNewConnection() {
    auto socket = nextPendingConnection();
    Connection* connection = new Connection(m_index, socket, this);
    m_connections.append(connection);
    connect(connection, &Connection::disconnected, [=]() { removeConnection(connection); });
    metrics()->onNewConnection();
    qDebug() << "Connected to" << connection->client();
}
