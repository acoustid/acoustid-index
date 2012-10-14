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

Connection::Connection(IndexSharedPtr index, QTcpSocket *socket, QObject *parent)
	: QObject(parent), m_socket(socket), m_output(socket), m_index(index),
		m_handler(NULL), m_refs(1),
		m_topScorePercent(10), m_maxResults(500)
{
	m_socket->setParent(this);
	m_client = QString("%1:%2").arg(m_socket->peerAddress().toString()).arg(m_socket->peerPort());
	qDebug() << "Connected to" << m_client;
	connect(m_socket, SIGNAL(readyRead()), SLOT(readIncomingData()));
	connect(m_socket, SIGNAL(disconnected()), SLOT(maybeDelete()));
}

Connection::~Connection()
{
	emit closed(this);
	qDebug() << "Disconnected from" << m_client;
}

Listener *Connection::listener() const
{
	return qobject_cast<Listener *>(parent());
}

bool Connection::maybeDelete()
{
	m_refs--;
	if (!m_refs) {
		deleteLater();
		return true;
	}
	return false;
}

void Connection::close()
{
	m_socket->disconnectFromHost();
}

void Connection::sendResponse(const QString& response, bool next)
{
	m_output << response << kCRLF << flush;
	if (next) {
		readIncomingData();
	}
}

void Connection::sendHandlerResponse(const QString& response, bool next)
{
	m_output << response << kCRLF << flush;
	m_handler = NULL;
	if (!maybeDelete()) {
		if (next) {
			readIncomingData();
		}
	}
}

void Connection::handleLine(const QString& line)
{
	//qDebug() << "Got line" << line << "from" << m_client;

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
	else if (command == "set") {
		if (args.size() < 2) {
			sendResponse("ERR expected 2 arguments");
			return;
		}
		if (args[0] == "max_results") {
			m_maxResults = args[1].toInt();
		}
		else if (args[0] == "top_score_percent") {
			m_topScorePercent = args[1].toInt();
		}
		else if (args[0] == "attrib" || args[0] == "attribute") {
			if (args.size() < 3) {
				sendResponse("ERR expected 3 arguments");
				return;
			}
			m_handler = new SetAttributeHandler(this, args);
		}
		else {
			sendResponse("ERR unknown parameter");
			return;
		}
	}
	else if (command == "get") {
		if (args.size() < 1) {
			sendResponse("ERR expected 1 argument");
			return;
		}
		if (args[0] == "max_results") {
			sendResponse(QString("OK %1 %2").arg(args[0]).arg(m_maxResults));
			return;
		}
		else if (args[0] == "top_score_percent") {
			sendResponse(QString("OK %1 %2").arg(args[0]).arg(m_topScorePercent));
			return;
		}
		else if (args[0] == "attrib" || args[0] == "attribute") {
			if (args.size() < 2) {
				sendResponse("ERR expected 2 arguments");
				return;
			}
			m_handler = new GetAttributeHandler(this, args);
		}
		else {
			sendResponse("ERR unknown parameter");
			return;
		}
	}
	else if (command == "echo") {
		m_handler = new EchoHandler(this, args);
	}
	else if (command == "search") {
		m_handler = new SearchHandler(this, args, m_maxResults, m_topScorePercent);
	}
	else if (command == "insert") {
		m_handler = new InsertHandler(this, args);
	}
	else if (command == "cleanup") {
		m_handler = new CleanupHandler(this, args);
	}
	else if (command == "optimize") {
		m_handler = new OptimizeHandler(this, args);
	}
	else if (command == "begin") {
		m_handler = new BeginHandler(this, args);
	}
	else if (command == "commit") {
		m_handler = new CommitHandler(this, args);
	}
	else if (command == "rollback") {
		m_handler = new RollbackHandler(this, args);
	}
	else {
		sendResponse("ERR unknown command");
		return;
	}

	m_refs++;
	connect(m_handler, SIGNAL(finished(QString)), SLOT(sendHandlerResponse(QString)), Qt::QueuedConnection);
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

