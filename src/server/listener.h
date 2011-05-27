// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_LISTENER_H_
#define ACOUSTID_SERVER_LISTENER_H_

#include <QTcpServer>
#include <QTcpSocket>
#include "index/index.h"
#include "store/directory.h"

namespace Acoustid {
namespace Server {

class Connection;

class Listener : public QTcpServer
{
	Q_OBJECT

public:
	Listener(const QString &path, QObject *parent = 0);
	~Listener();

	void stop();

signals:
	void lastConnectionClosed();

protected slots:
	void acceptNewConnection();
	void removeConnection(Connection*);

private:
	Directory* m_dir;
	Index* m_index;
	QList<Connection*> m_connections;
};

}
}

#endif
