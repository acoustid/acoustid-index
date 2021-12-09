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

	size_t length = argc - 2;
	uint32_t id = strtoul(argv[1], NULL, 10);
	qDebug() << "id=" << id;
	uint32_t *fp = new uint32_t[length];
	for (int i = 2; i < argc; i++) {
		fp[i - 2] = strtoul(argv[i], NULL, 10);
	}

    auto writer = index->openWriter();
	writer->addDocument(id, fp, length);
	writer->commit();

	return 0;
}

