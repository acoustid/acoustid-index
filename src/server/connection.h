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
	Connection(Acoustid::Index* index, QTcpSocket *socket, QObject *parent = 0);
	~Connection();

	Listener *listener() const;
	void close();

	Index* index() { return m_index; }
	IndexWriter* indexWriter() { return m_indexWriter.get(); }

	void setIndexWriter(IndexWriter* indexWriter)
	{
		m_indexWriter.reset(indexWriter);
	}

	QMutex* indexWriterMutex()
	{
		return &m_indexWriterMutex;
	}

signals:
	void closed(Connection *connection);

protected slots:
	void readIncomingData();
	void sendResponse(const QString& response, bool next = true);

	void handleLine(const QString& line);

private:
	QString m_client;
	QTcpSocket *m_socket;
	QString m_buffer;
	QTextStream m_output;
    Index* m_index;
    ScopedPtr<IndexWriter> m_indexWriter;
	QMutex m_indexWriterMutex;
	Handler* m_handler;
	int m_topScorePercent;
	int m_maxResults;
};

}
}

#endif

