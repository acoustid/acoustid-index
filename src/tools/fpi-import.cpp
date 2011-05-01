#include <stdint.h>
#include <stdio.h>
#include <QTextStream>
#include "index/index_writer.h"
#include "store/fs_directory.h"
#include "util/options.h"

using namespace Acoustid;

int main(int argc, char **argv)
{
	OptionParser parser("%prog [options]");
	parser.addOption("directory", 'd')
		.setArgument()
		.setHelp("index directory")
		.setMetaVar("DIR");
	parser.addOption("create", 'c')
		.setHelp("create an index in the directory");
	Options *opts = parser.parse(argc, argv);

	QString path = ".";
	if (opts->contains("directory")) {
		path = opts->option("directory");
	}

	FSDirectory dir(path);
	IndexWriter writer(&dir);
	try {
		writer.open(opts->contains("create"));
	}
	catch (IOException &ex) {
		qCritical() << "ERROR:" << ex.what();
		return 1;
	}

	QTextStream in(stdin);
	size_t counter = 0;
	while (!in.atEnd()) {
		QStringList line = in.readLine().split('|');
		if (line.size() != 2) {
			qWarning() << "Invalid line";
			continue;
		}
		int id = line.at(0).toInt();
		QString fpstr = line.at(1);
		if (fpstr.startsWith('{') && fpstr.endsWith('}')) {
			fpstr = fpstr.mid(1, fpstr.size() - 2);
		}
		QStringList fparr = fpstr.split(',');
		uint32_t fp[4096];
		for (int i = 0; i < fparr.size(); i++) {
			fp[i] = fparr.at(i).toInt();
		}
		writer.addDocument(id, fp, fparr.size());
		if (counter % 1000 == 0) {
			qDebug() << "Imported" << counter << "lines";
		}
		counter++;
	}
	writer.commit();

	return 0;
}

