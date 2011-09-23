#include <stdint.h>
#include <stdio.h>
#include "index/index.h"
#include "index/index_reader.h"
#include "index/top_hits_collector.h"
#include "store/fs_directory.h"
#include "util/options.h"
#include "util/timer.h"

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

	DirectorySharedPtr dir(new FSDirectory(path));
	IndexSharedPtr index;
	try {
		index = IndexSharedPtr(new Index(dir));
	}
	catch (IOException &ex) {
		qCritical() << "ERROR:" << ex.what();
		return 1;
	}

	ScopedPtr<IndexReader> reader(new IndexReader(index));

	QStringList args = opts->arguments();
	for (int i = 0; i < args.size(); i++) {
		QStringList arg = args.at(i).split(',');
		int32_t *fp = new int32_t[arg.size()];
		for (int j = 0; j < arg.size(); j++) {
			fp[j] = arg.at(j).toInt();
		}
		Timer timer;
		timer.start();
		TopHitsCollector collector(10);
		reader->search((uint32_t *)fp, arg.size(), &collector);
		qDebug() << "Search took" << timer.elapsed() << "ms";
		QList<Result> results = collector.topResults();
		for (int j = 0; j < results.size(); j++) {
			qDebug() << "Matches" << results[j].id() << results[j].score();
		}
	}

	return 0;
}

