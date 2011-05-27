// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QDebug>
#include <QCoreApplication>
#include "store/fs_directory.h"
#include "listener.h"
#include "connection.h"

using namespace Acoustid;

Listener::Listener(const QString &path, QObject *parent)
	: QTcpServer(parent)
{
	connect(this, SIGNAL(newConnection()), SLOT(acceptNewConnection()));
	m_dir = new FSDirectory(path);
    m_index = new Index(m_dir);
    m_index->open(true);
}

Listener::~Listener()
{
}

void Listener::stop()
{
	qDebug() << "Stopping" << this;
	connect(this, SIGNAL(lastConnectionClosed()), qApp, SLOT(quit()));
	close();
	foreach (Connection *connection, m_connections) {
		connection->close();
	}
}

void Listener::removeConnection(Connection *connection)
{
	qDebug() << "Removing connection" << connection;
	m_connections.removeAll(connection);
	if (m_connections.isEmpty()) {
		emit lastConnectionClosed();
	}
}

void Listener::acceptNewConnection()
{
	QTcpSocket *socket = nextPendingConnection();
	Connection *connection = new Connection(m_index, socket, this);
	m_connections.append(connection);
	qDebug() << "Adding connection" << connection;
	connect(connection, SIGNAL(closed(Connection *)), SLOT(removeConnection(Connection *)));
}

