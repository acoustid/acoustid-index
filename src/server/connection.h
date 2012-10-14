// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_CONNECTION_H_
#define ACOUSTID_SERVER_CONNECTION_H_

#include <QTextStream>
#include <QByteArray>
#include <QTcpSocket>
#include "index/index.h"
#include "index/index_writer.h"

namespace Acoustid {
namespace Server {

class Listener;
class Handler;

class Connection : public QObject
{
	Q_OBJECT

public:
	Connection(IndexSharedPtr index, QTcpSocket *socket, QObject *parent = 0);
	~Connection();

	Listener *listener() const;
	void close();

	IndexSharedPtr index() { return m_index; }
	IndexWriterSharedPtr indexWriter() { return m_indexWriter; }

	void setIndexWriter(IndexWriterSharedPtr indexWriter)
	{
		m_indexWriter = indexWriter;
	}

	QMutex* mutex()
	{
		return &m_mutex;
	}

protected:
	void sendResponse(const QString& response, bool next = true);

signals:
	void closed(Connection *connection);

protected slots:
	void readIncomingData();
	void sendHandlerResponse(const QString& response, bool next = true);

	void handleLine(const QString& line);
	bool maybeDelete();

private:
	QString m_client;
	QTcpSocket *m_socket;
	QString m_buffer;
	QTextStream m_output;
    IndexSharedPtr m_index;
    IndexWriterSharedPtr m_indexWriter;
	QMutex m_mutex;
	Handler* m_handler;
	int m_topScorePercent;
	int m_maxResults;
	int m_refs;
};

}
}

#endif

