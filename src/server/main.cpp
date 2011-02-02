#include <QCoreApplication>
#include "listener.h"

int main(int argc, char **argv)
{
	QCoreApplication app(argc, argv);
	Listener listener;
	listener.listen(QHostAddress("127.0.0.1"), 6000);
	qDebug() << "Listening on port" << listener.serverPort();
	return app.exec();
}

