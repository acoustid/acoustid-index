#ifndef ACOUSTID_LISTENER_H_
#define ACOUSTID_LISTENER_H_

#include <QTcpServer>
#include <QTcpSocket>
#include "store/fs_directory.h"
#include "index/index_writer.h"

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
	void removeConnection(Connection *);

private:
	Acoustid::FSDirectory *m_dir;
	Acoustid::IndexWriter *m_writer;
	QList<Connection *> m_connections;
};

#endif
