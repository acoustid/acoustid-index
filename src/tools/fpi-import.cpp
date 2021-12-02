#include <stdint.h>
#include <stdio.h>
#include <QTextStream>
#include "index/index.h"
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
	parser.addOption("cleanup", 'n')
		.setHelp("cleanup the index directory after importing the data");
	parser.addOption("optimize", 'o')
		.setHelp("optimize the index after importing the data");
	Options *opts = parser.parse(argc, argv);

	QString path = ".";
	if (opts->contains("directory")) {
		path = opts->option("directory");
	}

	DirectorySharedPtr dir(new FSDirectory(path));
	IndexSharedPtr index;
	try {
		index = IndexSharedPtr(new Index(dir, opts->contains("create")));
	}
	catch (IOException &ex) {
		qCritical() << "ERROR:" << ex.what();
		return 1;
	}

	std::unique_ptr<IndexWriter> writer(new IndexWriter(index));

	const size_t lineSize = 1024 * 1024;
	char line[lineSize];
    std::vector<uint32_t> fp;
    fp.reserve(10 * 1024);

	size_t counter = 0;
	while (fgets(line, lineSize, stdin) != NULL) {
		char *ptr = line;
		long id = strtol(ptr, &ptr, 10);
		if (*ptr++ != '|') {
			qWarning() << "Invalid line 1";
			continue;
		}
		if (*ptr != '{') {
			qWarning() << "Invalid line 2";
			continue;
		}
        fp.clear();
		while (*ptr != '}' && *ptr != 0) {
			ptr++;
			fp.push_back(strtol(ptr, &ptr, 10));
			if (*ptr != ',' && *ptr != '}' && *ptr != 0) {
				qWarning() << "Invalid line 3" << int(*ptr);
				continue;
			}
		}
		writer->insertOrUpdateDocument(id, fp);
		if (counter % 1000 == 0) {
			qDebug() << "Imported" << counter << "lines";
		}
		counter++;
	}
	writer->commit();

	if (opts->contains("optimize")) {
		qDebug() << "Optimizing the index";
		writer->optimize();
	}

	if (opts->contains("cleanup")) {
		qDebug() << "Cleaning up the index directory";
		writer->cleanup();
	}

	return 0;
}

