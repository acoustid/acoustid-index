#include <stdint.h>
#include <stdio.h>
#include "index/index.h"
#include "index/index_writer.h"
#include "store/fs_directory.h"

using namespace Acoustid;

int main(int argc, char **argv)
{
	DirectorySharedPtr dir(new FSDirectory("."));
	IndexSharedPtr index(new Index(dir, true));

	std::unique_ptr<IndexWriter> writer(new IndexWriter(index));

	size_t length = argc - 2;
	uint32_t id = strtoul(argv[1], NULL, 10);
	qDebug() << "id=" << id;
	QVector<uint32_t> fp(length);
	for (int i = 2; i < argc; i++) {
		fp[i - 2] = strtoul(argv[i], NULL, 10);
	}

	writer->insertOrUpdateDocument(id, fp);
	writer->commit();

	return 0;
}

