// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QCoreApplication>
#include <QThreadPool>
#include "qhttpserver.hpp"
#include "qhttpserverrequest.hpp"
#include "qhttpserverresponse.hpp"
#include "util/options.h"
#include "listener.h"
#include "metrics.h"
#include "http.h"

using namespace Acoustid;
using namespace Acoustid::Server;

using namespace qhttp::server;

int main(int argc, char **argv)
{
	OptionParser parser("%prog [options]");
	parser.addOption("directory", 'd')
		.setArgument()
		.setHelp("index directory")
		.setMetaVar("DIR")
		.setDefaultValue(".");
	parser.addOption("address", 'a')
		.setArgument()
		.setHelp("listen on this address (default: 127.0.0.1)")
		.setDefaultValue("127.0.0.1");
	parser.addOption("port", 'p')
		.setArgument()
		.setHelp("listen on this port (default: 6080)")
		.setDefaultValue("6080");
	parser.addOption("http-address")
		.setArgument()
		.setHelp("http server listens on this address (default: 127.0.0.1)")
        .setMetaVar("ADDRESS")
		.setDefaultValue("127.0.0.1");
	parser.addOption("http-port")
		.setArgument()
		.setHelp("http server listens on this port (default: 6081)")
        .setMetaVar("PORT")
		.setDefaultValue("6081");
	parser.addOption("mmap", 'm')
		.setHelp("use mmap to read index files");
	parser.addOption("threads", 't')
		.setArgument()
		.setHelp("use specific number of threads")
		.setDefaultValue("0");
	std::unique_ptr<Options> opts(parser.parse(argc, argv));

	QString path = opts->option("directory");

	QString address = opts->option("address");
	int port = opts->option("port").toInt();

	QString httpAddress = opts->option("http-address");
	int httpPort = opts->option("http-port").toInt();

	QCoreApplication app(argc, argv);

	int numThreads = opts->option("threads").toInt();
	if (numThreads) {
		QThreadPool::globalInstance()->setMaxThreadCount(numThreads);
	}

	auto metrics = QSharedPointer<Metrics>(new Metrics());

	Listener::setupSignalHandlers();

	Listener listener(path, opts->contains("mmap"));
	listener.setMetrics(metrics);
	listener.listen(QHostAddress(address), port);
	qDebug() << "Simple server listening on" << address << "port" << port;

    HttpRequestHandler handler(listener.index(), metrics);

	QHttpServer httpListener(&app);
    httpListener.listen(QHostAddress(httpAddress), httpPort, [&](QHttpRequest *req, QHttpResponse *res) {
        handler.router().handle(req, res);
    });
    qDebug() << "HTTP server listening on" << httpAddress << "port" << httpPort;
    qDebug() << "Prometheus metrics available at" << QString("http://%1:%2/_metrics").arg(httpAddress).arg(httpPort);

	return app.exec();
}

