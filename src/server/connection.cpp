// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QThreadPool>
#include "listener.h"
#include "connection.h"
#include "handler.h"
#include "handlers.h"

using namespace Acoustid;
using namespace Acoustid::Server;

static const char* kCRLF = "\r\n";
static const int kMaxLineSize = 1024 * 32;

Connection::Connection(Index* index, QTcpSocket *socket, QObject *parent)
	: QObject(parent), m_socket(socket), m_output(socket), m_index(index), m_indexWriter(NULL), m_handler(NULL)
{
	qDebug() << "Connected to client" << m_socket->peerAddress().toString() << "on port" << m_socket->peerPort();
	m_socket->setParent(this);
	connect(m_socket, SIGNAL(readyRead()), SLOT(readIncomingData()));
	connect(m_socket, SIGNAL(disconnected()), SLOT(deleteLater()));
}

Connection::~Connection()
{
	emit closed(this);
	qDebug() << "Disconnected";
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
	m_output << response << kCRLF << flush;
	m_handler = NULL;
	if (next) {
		readIncomingData();
	}
}

void Connection::handleLine(const QString& line)
{
	qDebug() << "Got line" << line;

	QString command;
	QStringList args;
	int pos = line.indexOf(' ');
	if (pos == -1) {
		command = line.toLower();
	}
	else {
		command = line.left(pos).toLower();
		args = line.mid(pos + 1).split(' ');
	}

	if (command.isEmpty()) {
		sendResponse("ERR missing command");
		return;
	}
	else if (command == "kill") {
		sendResponse("OK", false);
		listener()->stop();
		return;
	}
	else if (command == "quit") {
		sendResponse("OK", false);
		close();
		return;
	}
	else if (command == "echo") {
		m_handler = new EchoHandler(this, args);
	}
	else if (command == "search") {
		m_handler = new SearchHandler(this, args);
	}
	else if (command == "insert") {
		m_handler = new InsertHandler(this, args);
	}
	else if (command == "begin") {
		m_handler = new BeginHandler(this, args);
	}
	else if (command == "commit") {
		m_handler = new CommitHandler(this, args);
	}
	else {
		sendResponse("ERR unknown command");
		return;
	}

	connect(m_handler, SIGNAL(finished(QString)), SLOT(sendResponse(QString)), Qt::QueuedConnection);
	QThreadPool::globalInstance()->start(m_handler);
}

void Connection::readIncomingData()
{
	if (m_handler) {
		qWarning() << "Got data while still handling the previous command";
		return;
	}

	m_buffer += m_output.readAll();
	int pos = m_buffer.indexOf(kCRLF);
	if (pos == -1) {
		if (m_buffer.size() > kMaxLineSize) {
			sendResponse("ERR line too long", false);
			close();
		}
		return;
	}
	QString line = m_buffer.left(pos);
	m_buffer = m_buffer.mid(pos + 2);
	handleLine(line);
}

