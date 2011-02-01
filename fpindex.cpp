#include <QFile>
#include <QTextStream>
#include <QDebug>
#include <stdint.h>
#include <stdio.h>

#include "index/segment_data_writer.h"
#include "index/segment_index_writer.h"
#include "store/fs_output_stream.h"


int main(int argc, char **argv)
{
	QScopedPointer<OutputStream> indexStream(FSOutputStream::open("segment0.fii"));
	QScopedPointer<OutputStream> dataStream(FSOutputStream::open("segment0.fid"));

	SegmentIndexWriter indexWriter(indexStream.data());
	indexWriter.setBlockSize(256);
	indexWriter.setIndexInterval(256);

	SegmentDataWriter dataWriter(dataStream.data(), &indexWriter, indexWriter.blockSize());

	size_t itemCount = 0;
	QTextStream in(stdin);
	while (!in.atEnd()) {
		uint32_t key, value;
		in >> key >> value;
		if (in.status() != QTextStream::Ok)
			break;
		if (itemCount % 100000 == 0) {
			qDebug() << itemCount;
		}
		dataWriter.addItem(key, value);
		itemCount++;
	}

	return 0;
}

