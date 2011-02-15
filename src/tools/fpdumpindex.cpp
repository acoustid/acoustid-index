#include <QFile>
#include <QTextStream>
#include <QDebug>
#include <QList>
#include <QTextStream>
#include <stdio.h>
#include "index/segment_index.h"
#include "index/segment_index_reader.h"
#include "store/mmap_input_stream.h"

using namespace Acoustid;

int main(int argc, char **argv)
{
	ScopedPtr<InputStream> inputStream(MMapInputStream::open("segment0.fii"));
	ScopedPtr<SegmentIndexReader> indexReader(new SegmentIndexReader(inputStream.get()));
	ScopedPtr<SegmentIndex> index(indexReader->read());

	QTextStream out(stdout);
	for (size_t i = 0; i < index->levelKeyCount(0); i++) {
		out << index->levelKey(i) << endl;
	}

	return 0;
}

