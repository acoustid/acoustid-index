// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <algorithm>
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
	std::sort(fingerprint, fingerprint + length);
	size_t i = 0, block = 0, lastBlock = SIZE_MAX;
	while (i < length) {
		if (block > lastBlock || lastBlock == SIZE_MAX) {
			size_t localFirstBlock, localLastBlock;
			if (m_index->search(fingerprint[i], &localFirstBlock, &localLastBlock)) {
				if (block > localLastBlock) {
					i++;
					continue;
				}
				block = std::max(block, localFirstBlock);
				lastBlock = localLastBlock;
			}
			else {
				i++;
				continue;
			}
		}
		uint32_t firstKey = m_index->levelKey(block);
		uint32_t lastKey = block + 1 < m_index->levelKeyCount(0) ? m_index->levelKey(block + 1) : UINT32_MAX;
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
			else if (lastKey < fingerprint[i]) {
				break;
			}
		}
		block++;
	}
}

