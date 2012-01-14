// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_LISTENER_H_
#define ACOUSTID_SERVER_LISTENER_H_

#include <QTcpServer>
#include <QTcpSocket>
#include <QSocketNotifier>
#include "index/index.h"
#include "store/directory.h"

namespace Acoustid {
namespace Server {

class Connection;

class Listener : public QTcpServer
{
	Q_OBJECT

public:
	Listener(const QString &path, bool mmap = false, QObject *parent = 0);
	~Listener();

	void stop();

	static void setupSignalHandlers();
	static void setupLogging(bool syslog, const QString& facility);

signals:
	void lastConnectionClosed();

protected slots:
	void acceptNewConnection();
	void removeConnection(Connection*);

	void handleSigInt();
	void handleSigTerm();

private:
	static int m_sigIntFd[2];
	static int m_sigTermFd[2];
	static void sigIntHandler(int unused);
	static void sigTermHandler(int unused);

	DirectorySharedPtr m_dir;
	IndexSharedPtr m_index;
	QList<Connection*> m_connections;
	QSocketNotifier *m_sigIntNotifier;
	QSocketNotifier *m_sigTermNotifier;
};

}
}

#endif
