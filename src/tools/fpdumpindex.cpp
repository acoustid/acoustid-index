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
	std::unique_ptr<InputStream> inputStream(MMapInputStream::open("segment0.fii"));
	std::unique_ptr<SegmentIndexReader> indexReader(new SegmentIndexReader(inputStream.get()));
	SegmentIndexSharedPtr index(indexReader->read());

	QTextStream out(stdout);
	for (size_t i = 0; i < index->levelKeyCount(0); i++) {
		out << index->levelKey(i) << endl;
	}

	return 0;
}

