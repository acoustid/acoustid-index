#include <stdint.h>
#include <stdio.h>

#include <QDebug>
#include <QFile>
#include <QTextStream>

#include "index/segment_data_writer.h"
#include "index/segment_index_writer.h"
#include "store/fs_output_stream.h"

using namespace Acoustid;

int main(int argc, char **argv) {
    std::unique_ptr<OutputStream> indexStream(FSOutputStream::open("segment0.fii"));
    std::unique_ptr<OutputStream> dataStream(FSOutputStream::open("segment0.fid"));

    SegmentIndexWriter indexWriter(indexStream.get());
    indexWriter.setBlockSize(512);

    SegmentDataWriter dataWriter(dataStream.get(), &indexWriter, indexWriter.blockSize());

    size_t itemCount = 0;
    QTextStream in(stdin);
    while (!in.atEnd()) {
        uint32_t key, value;
        in >> key >> value;
        if (in.status() != QTextStream::Ok) break;
        if (itemCount % 100000 == 0) {
            qDebug() << itemCount;
        }
        dataWriter.addItem(key, value);
        itemCount++;
    }

    return 0;
}
