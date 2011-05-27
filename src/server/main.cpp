// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <QCoreApplication>
#include "util/options.h"
#include "listener.h"

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
		.setHelp("listen on this port (default: 6000)")
		.setDefaultValue("6000");
	Options *opts = parser.parse(argc, argv);

	QString path = opts->option("directory");
	QString address = opts->option("address");
	int port = opts->option("port").toInt();

	QCoreApplication app(argc, argv);
	Listener listener(path);
	listener.listen(QHostAddress(address), port);
	qDebug() << "Listening on" << address << "port" << port;
	return app.exec();
}

