// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QCoreApplication>
#include <QThreadPool>
#include "util/options.h"
#include "listener.h"
#include "metrics_server.h"

using namespace Acoustid;
using namespace Acoustid::Server;

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
	parser.addOption("metrics-address")
		.setArgument()
		.setHelp("prometheus metrics listen on this address (default: 127.0.0.1)")
		.setDefaultValue("127.0.0.1");
	parser.addOption("metrics-port")
		.setArgument()
		.setHelp("prometheus metrics listen on this port (default: 6081)")
		.setDefaultValue("6081");
	parser.addOption("mmap", 'm')
		.setHelp("use mmap to read index files");
	parser.addOption("syslog", 's')
		.setHelp("log to syslog");
	parser.addOption("syslog-facility", 'f')
		.setArgument()
		.setMetaVar("FACILITY")
		.setHelp("syslog facility to use (default: user)")
		.setDefaultValue("user");
	parser.addOption("threads", 't')
		.setArgument()
		.setHelp("use specific number of threads")
		.setDefaultValue("0");
	ScopedPtr<Options> opts(parser.parse(argc, argv));

	QString path = opts->option("directory");

	QString address = opts->option("address");
	int port = opts->option("port").toInt();

	QString metricsAddress = opts->option("metrics-address");
	int metricsPort = opts->option("metrics-port").toInt();

	QCoreApplication app(argc, argv);

	int numThreads = opts->option("threads").toInt();
	if (numThreads) {
		QThreadPool::globalInstance()->setMaxThreadCount(numThreads);
	}

    auto metrics = QSharedPointer<Metrics>(new Metrics());
    auto metricsServer = QSharedPointer<MetricsServer>(new MetricsServer(metrics));

	Listener::setupSignalHandlers();
	Listener::setupLogging(opts->contains("syslog"), opts->option("syslog-facility"));

	Listener listener(path, opts->contains("mmap"));
	listener.setMetrics(metrics);
	listener.listen(QHostAddress(address), port);
	qDebug() << "Index server listening on" << address << "port" << port;

    metricsServer->start(QHostAddress(metricsAddress), metricsPort);
	qDebug() << "Prometheus metrics available at" << QString("http://%1:%2/metrics").arg(metricsAddress).arg(metricsPort);

	return app.exec();
}

