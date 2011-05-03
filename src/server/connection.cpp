#include <QDebug>
#include <QTcpSocket>
#include <QHostAddress>
#include "listener.h"
#include "connection.h"
#include "index/top_hits_collector.h"

using namespace Acoustid;

static const char* kCRLF = "\r\n";
static const int kMaxLineSize = 1024 * 32;

Connection::Connection(IndexWriter *writer, QTcpSocket *socket, QObject *parent)
	: QObject(parent), m_socket(socket), m_handler(0), m_output(socket), m_writer(writer)
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

void Connection::handleLine(const QString& line)
{
	qDebug() << "Got line" << line;

	QString command, params;
	int pos = line.indexOf(' ');
	if (pos == -1) {
		command = line.toLower();
	}
	else {
		command = line.left(pos).toLower();
		params = line.mid(pos + 1);
	}

	qDebug() << "Got command" << command;

	if (command == "shutdown") {
		m_output << "OK" << kCRLF;
		listener()->stop();
	}
	else if (command == "quit") {
		m_output << "OK" << kCRLF;
		close();
	}
	else if (command == "echo") {
		m_output << "OK " << params << kCRLF;
	}
	else if (command == "search") {
		TopHitsCollector collector(10);
		QStringList arg = params.split(',');
		int32_t *fp = new int32_t[arg.size()];
		size_t fpsize = 0;
		for (int j = 0; j < arg.size(); j++) {
			bool ok;
			fp[fpsize] = arg.at(j).toInt(&ok);
			if (ok) {
				fpsize++;
			}
		}
		if (!fpsize) {
			m_output << "ERR missing fingerprint" << kCRLF;
		}
		else {
			m_writer->search((uint32_t *)fp, fpsize, &collector);
			m_output << "OK";
			QList<Result> results = collector.topResults();
			for (int j = 0; j < results.size(); j++) {
				m_output << " " << results[j].id() << ":" << results[j].score();
			}
			m_output << kCRLF;
		}
	}
	else if (command == "add") {
	//	m_writer->commit();
	}
	else if (command == "commit") {
		m_writer->commit();
		m_output << "OK" << kCRLF;
	}
	else if (command.isEmpty()) {
		m_output << "ERR missing command" << kCRLF;
	}
	else {
		m_output << "ERR unknown command " << command << kCRLF;
	}
}

void Connection::readIncomingData()
{
	if (m_handler) {
		qWarning() << "Got data while still handling the previous command";
		return;
	}

	m_buffer += m_output.readAll();
	while (true) {
		int pos = m_buffer.indexOf(kCRLF);
		if (pos == -1) {
			if (m_buffer.size() > kMaxLineSize) {
				m_output << "ERR line too long" << kCRLF;
				close();
			}
			break;
		}
		handleLine(m_buffer.left(pos));
		m_output << flush;
		m_buffer = m_buffer.mid(pos + 2);
	}
}

