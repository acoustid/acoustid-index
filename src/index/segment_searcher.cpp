// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/output_stream.h"
#include "collector.h"
#include "segment_index.h"
#include "segment_data_reader.h"
#include "segment_searcher.h"

using namespace Acoustid;

SegmentSearcher::SegmentSearcher(SegmentIndex *index, SegmentDataReader *dataReader)
	: m_index(index), m_dataReader(dataReader)
{
}

SegmentSearcher::~SegmentSearcher()
{
	delete m_index;
	delete m_dataReader;
}

void SegmentSearcher::search(uint32_t *fingerprint, size_t length, Collector *collector)
{
	// XXX do this in two phases, optimizing multiple reads of the same block
	size_t lastReadBlock = ~0;
	size_t readBlockCount = 0;
	for (size_t i = 0; i < length; i++) {
		size_t firstBlock, lastBlock;
		if (m_index->search(fingerprint[i], &firstBlock, &lastBlock)) {
			if (readBlockCount > 0 && lastReadBlock >= firstBlock) {
				firstBlock = lastReadBlock + 1;
			}
			for (size_t block = firstBlock; block <= lastBlock; block++) {
				uint32_t firstKey = m_index->levelKey(block);
				ScopedPtr<BlockDataIterator> blockData(m_dataReader->readBlock(block, firstKey));
				while (blockData->next()) {
					uint32_t key = blockData->key();
					if (key >= fingerprint[i]) {
						if (key == fingerprint[i]) {
							collector->collect(blockData->value());
						}
						break;
					}
				}
				readBlockCount++;
				lastReadBlock = block;
			}
		}
	}
}

