#include <QFile>
#include <QTextStream>
#include <QDebug>
#include <QList>
#include <QTextStream>
#include <stdio.h>
#include "util/options.h"
#include "index/index_reader.h"
#include "store/fs_directory.h"

using namespace Acoustid;

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

	FSDirectory dir(path);
	IndexReader reader(&dir);
	try {
		reader.open();
	}
	catch (IOException &ex) {
		qCritical() << "ERROR:" << ex.what();
		return 1;
	}

	QTextStream out(stdout);
	out << "Revision: " << reader.revision() << endl;
	const SegmentInfoList& infos = reader.segmentInfos();
	out << "Segments: " << infos.size() << endl;
	for (int i = 0; i < infos.size(); i++) {
		const SegmentInfo& info = infos.info(i);
		out << "Segment " << info.id() << ": " << info.numDocs() << endl;
	}

	return 0;
}

