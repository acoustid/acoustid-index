#ifndef ACOUSTID_LISTENER_H_
#define ACOUSTID_LISTENER_H_

#include <QTcpServer>
#include <QTcpSocket>

class Connection;

class Listener : public QTcpServer
{
	Q_OBJECT

public:
	Listener(QObject *parent = 0);
	~Listener();

	void stop();

signals:
	void lastConnectionClosed();

protected slots:
	void acceptNewConnection();
	void removeConnection(Connection *);

private:
	QList<Connection *> m_connections;
};

#endif
