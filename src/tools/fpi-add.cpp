#include <stdint.h>
#include <stdio.h>
#include "index/index.h"
#include "index/index_writer.h"
#include "store/fs_directory.h"

using namespace Acoustid;

int main(int argc, char **argv)
{
	FSDirectory dir(".");
	Index index(&dir);
	index.open(true);

	ScopedPtr<IndexWriter> writer(index.createWriter());

	size_t length = argc - 2;
	uint32_t id = strtoul(argv[1], NULL, 10);
	qDebug() << "id=" << id;
	uint32_t *fp = new uint32_t[length];
	for (int i = 2; i < argc; i++) {
		fp[i - 2] = strtoul(argv[i], NULL, 10);
	}

	writer->addDocument(id, fp, length);
	writer->commit();

	return 0;
}

