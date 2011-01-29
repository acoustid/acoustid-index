#include <QDebug>
#include <QTcpSocket>
#include <QHostAddress>
#include "listener.h"
#include "connection.h"

Connection::Connection(QTcpSocket *socket, QObject *parent)
	: QObject(parent), m_socket(socket), m_stream(socket), m_handler(0)
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

void Connection::readIncomingData()
{
	if (m_handler) {
		qWarning() << "Got data while still handling the previous command";
		return;
	}

	QString line = m_stream.readLine();
	if (line.isNull()) {
		// Not enough data
		return;
	}

	QString command, params;
	int pos = line.indexOf(' ');
	if (pos == -1) {
		command = line.toUpper();
	}
	else {
		command = line.left(pos).toUpper();
		params = line.mid(pos + 1);
	}

	qDebug() << "Got command" << command;

	if (command == "QUIT") {
		m_stream << "OK" << endl;
		listener()->stop();
	}
	else if (command == "ECHO") {
		m_stream << "OK " << params << endl;
	}
	else {
		m_stream << "ERROR Unknown command" << endl;
	}
}

