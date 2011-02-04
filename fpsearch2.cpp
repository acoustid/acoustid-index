#include <QFile>
#include <QTextStream>
#include <QDebug>
#include <QList>
#include <QSet>
#include <stdint.h>
#include <stdio.h>
#include <algorithm>
#include "index/segment_data_reader.h"
#include "index/segment_index.h"
#include "index/segment_index_reader.h"
#include "store/mmap_input_stream.h"
#include "store/fs_input_stream.h"
#include "util/timer.h"

int main(int argc, char **argv)
{
	//InputStream *inputStream = FSInputStream::open("segment0.fii");
	InputStream *inputStream = MMapInputStream::open("segment0.fii");
	InputStream *dataInputStream = MMapInputStream::open("segment0.fid");
	SegmentIndexReader *indexReader = new SegmentIndexReader(inputStream);
	SegmentIndex* index = indexReader->read();

	qDebug() << "BlockSize =" << index->blockSize();
	qDebug() << "IndexInterval =" << index->indexInterval();
	qDebug() << "KeyCount0 =" << index->levelKeyCount(0);

//	dataInputStream->setBufferSize(index->blockSize());
	SegmentDataReader *dataReader = new SegmentDataReader(dataInputStream, index->blockSize());

	uint32_t fp[] = {
		-965422978,-952364817,-986246953,-1003080754,1144403678,1142324974,1414823663,1414851167,1431629645,1448451853,1180141324,1180013404,1198912380,-948603939,-982035489,-984201729,-983640721,-975313553,-979507921,-943721201,-944390129,-961167297,-957034385,-969649937,-986377993,-1003081266,-1003080993,-1005175057,-1001054481,-992728451,-975934900,-942276852,-958937332,-963128548,-943852740,-943857284,-2017463811,-2051091969,-2049075730,-2023965202,-2023965330,-2015457978,-2097249210,-1024024562,-1033506786,-1020440466,-1052831497,-1070190130,1144402654,1142298350,1415012079,1423244015,1440016973,1465167693,1448580940,1180144460,1180013436,-967511043,-950570019,-984198689,-984197777,-992608913,-711070417,-711465713,-944258033,-944390129,-961228689,-965422993,-952889105,-986304034,-1003081010,-1005177090,-1000972561,-1001096593,-975934900,-950736116,-967319796,-967342308,-959065284,-940183188,-943721988,-2019569185,-2053127954,-2049141522,-2057524114,-2023908242,-2015984562,-956895154,-956829618,-969584529,-952299281,-985722465,1144402830,1412839166,1414930159,1423212271,1423206989,1473572429,1456848717,1180141324,1182110556,1182135165,-948603939,-948481569,-984201873,-1000876689,-992154321,-711072449,-675818225,-944390129,-961232833,-957034385,-952876817,-986246913,-1003080818,-1005178210,-1005183250,-1001055505,-992728355,-975934516
	};
	size_t fpSize = sizeof(fp) / sizeof(fp[0]);
	std::sort(fp, fp + fpSize);

	Timer timer;
	timer.start();
	size_t lastReadBlock = ~0;
	size_t readBlockCount = 0;
	for (size_t i = 0; i < fpSize; i++) {
		size_t firstBlock, lastBlock;
		//qDebug() << "searching for key" << fp[i];
		if (index->search(fp[i], &firstBlock, &lastBlock)) {
			//qDebug() << "found blocks" << firstBlock << lastBlock;
			if (readBlockCount > 0 && lastReadBlock >= firstBlock) {
				firstBlock = lastReadBlock + 1;
			}
			if (firstBlock <= lastBlock) {
				//qDebug() << "searching for key" << fp[i];
				//qDebug() << "reading blocks" << firstBlock << lastBlock << (ssize_t(index->levelKey(firstBlock)) - ssize_t(fp[i])) << (ssize_t(index->levelKey(lastBlock)) - ssize_t(fp[i]));
			}
			for (size_t block = firstBlock; block <= lastBlock; block++) {
				uint32_t firstKey = index->levelKey(block);
				//qDebug() << "  - reading block" << block << ssize_t(firstKey - fp[i]);
				BlockDataIterator *blockData = dataReader->readBlock(block, firstKey);
				while (blockData->next()) {
					uint32_t key = blockData->key();
					if (key >= fp[i]) {
						if (key == fp[i]) {
							//qDebug() << "got key with block" << block << "and fingerprint id" << blockData->value();
						}
						break;
					}
				}
				readBlockCount++;
				lastReadBlock = block;
			}
		}
	}

	qDebug() << "Index search took" << timer.elapsed() << "ms";
	qDebug() << "Had to read" << readBlockCount << "blocks";

	delete indexReader;
	delete inputStream;
	delete index;

	return 0;
}

