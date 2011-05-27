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
		.setMetaVar("DIR");
	Options *opts = parser.parse(argc, argv);

	QString path = ".";
	if (opts->contains("directory")) {
		path = opts->option("directory");
	}

	QCoreApplication app(argc, argv);
	Listener listener(path);
	listener.listen(QHostAddress("127.0.0.1"), 6000);
	qDebug() << "Listening on port" << listener.serverPort();
	return app.exec();
}

