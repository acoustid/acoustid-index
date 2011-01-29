#ifndef ACOUSTID_CONNECTION_H_
#define ACOUSTID_CONNECTION_H_

#include <QTextStream>

class QTcpSocket;
class Handler;
class Listener;

class Connection : public QObject
{
	Q_OBJECT

public:
	Connection(QTcpSocket *socket, QObject *parent = 0);
	~Connection();

	Listener *listener() const;
	void close();

signals:
	void closed(Connection *connection);

protected slots:
	void readIncomingData();

private:
	QTcpSocket *m_socket;
	QTextStream m_stream;
	Handler *m_handler;
};

#endif

