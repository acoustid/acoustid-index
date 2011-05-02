// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/output_stream.h"
#include "collector.h"
#include "segment_index.h"
#include "segment_data_reader.h"
#include "segment_searcher.h"

using namespace Acoustid;

SegmentSearcher::SegmentSearcher(SegmentIndexSharedPtr index, SegmentDataReader *dataReader)
	: m_index(index), m_dataReader(dataReader)
{
}

SegmentSearcher::~SegmentSearcher()
{
	delete m_dataReader;
}

void SegmentSearcher::search(uint32_t *fingerprint, size_t length, Collector *collector)
{
	if (!length) {
		return;
	}

	size_t lastReadBlock = ~0;
	size_t readBlockCount = 0;
	qSort(fingerprint, fingerprint + length);
	QList<size_t> blocks;
	for (size_t i = 0; i < length; i++) {
		size_t firstBlock, lastBlock;
		if (m_index->search(fingerprint[i], &firstBlock, &lastBlock)) {
			if (readBlockCount > 0 && lastReadBlock >= firstBlock) {
				firstBlock = lastReadBlock + 1;
			}
			for (size_t block = firstBlock; block <= lastBlock; block++) {
				blocks.append(block);
				readBlockCount++;
				lastReadBlock = block;
			}
		}
	}
	size_t i = 0;
	for (size_t b = 0; b < blocks.size(); b++) {
		size_t block = blocks.at(b);
		uint32_t firstKey = m_index->levelKey(block);
		ScopedPtr<BlockDataIterator> blockData(m_dataReader->readBlock(block, firstKey));
		while (blockData->next()) {
			uint32_t key = blockData->key();
			while (key > fingerprint[i]) {
				i++;
				if (i >= length) {
					return;
				}
			}
			if (key == fingerprint[i]) {
				collector->collect(blockData->value());
			}
		}
	}
}

