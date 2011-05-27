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

class Connection : public QObject
{
	Q_OBJECT

public:
	Connection(Acoustid::Index* index, QTcpSocket *socket, QObject *parent = 0);
	~Connection();

	Listener *listener() const;
	void close();

signals:
	void closed(Connection *connection);

protected slots:
	void readIncomingData();

	void handleLine(const QString& line);

private:
	QTcpSocket *m_socket;
	QString m_buffer;
	QTextStream m_output;
    Acoustid::Index* m_index;
    Acoustid::IndexWriter* m_indexWriter;
};

}
}

#endif

