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
class Metrics;

class Listener : public QTcpServer
{
	Q_OBJECT

public:
	Listener(const QSharedPointer<Index>& index, const QSharedPointer<Metrics>& metrics, QObject *parent = 0);
	~Listener();

	void stop();

    QSharedPointer<Metrics> metrics() const { return m_metrics; }

    QSharedPointer<Index> index() const { return m_index; }

	static void setupSignalHandlers();

signals:
	void lastConnectionClosed();

protected:
	void acceptNewConnection();

	void handleSigInt();
	void handleSigTerm();

private:
	static int m_sigIntFd[2];
	static int m_sigTermFd[2];
	static void sigIntHandler(int unused);
	static void sigTermHandler(int unused);

    void removeConnection(Connection *conn);

	DirectorySharedPtr m_dir;
	IndexSharedPtr m_index;
    QSharedPointer<Metrics> m_metrics;
	QList<Connection*> m_connections;
	QSocketNotifier *m_sigIntNotifier;
	QSocketNotifier *m_sigTermNotifier;
};

}
}

#endif
