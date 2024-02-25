// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <algorithm>
#include "segment_data_reader.h"
#include "segment_searcher.h"

using namespace Acoustid;

SegmentSearcher::SegmentSearcher(SegmentIndexSharedPtr index, SegmentDataReader *dataReader, uint32_t lastKey)
	: m_index(index), m_dataReader(dataReader), m_lastKey(lastKey)
{
}

SegmentSearcher::~SegmentSearcher()
{
}

void SegmentSearcher::search(const std::vector<uint32_t> &hashes, std::unordered_map<uint32_t, int> &hits)
{
	size_t i = 0, block = 0, lastBlock = SIZE_MAX;
	while (i < hashes.size()) {
		if (block > lastBlock || lastBlock == SIZE_MAX) {
			size_t localFirstBlock, localLastBlock;
			if (hashes[i] > m_lastKey) {
				// All following items are larger than the last segment's key.
				return;
			}
			if (m_index->search(hashes[i], &localFirstBlock, &localLastBlock)) {
				if (block > localLastBlock) {
					// We already searched this block and the fingerprint item was not found.
					i++;
					continue;
				}
				// Don't search for the same block multiple times.
				block = std::max(block, localFirstBlock);
				lastBlock = localLastBlock;
			}
			else {
				// The fingerprint item is definitely not in any block.
				i++;
				continue;
			}
		}
		uint32_t firstKey = m_index->key(block);
		uint32_t lastKey = block + 1 < m_index->blockCount() ? m_index->key(block + 1) : m_lastKey + 1;
		std::unique_ptr<BlockDataIterator> blockData(m_dataReader->readBlock(block, firstKey));
		while (blockData->next()) {
			uint32_t key = blockData->key();
			if (key >= hashes[i]) {
				while (key > hashes[i]) {
					i++;
					if (i >= hashes.size()) {
						return;
					}
					else if (lastKey < hashes[i]) {
						// There are no longer any items in this block that we could match.
						goto nextBlock;
					}
				}
				if (key == hashes[i]) {
                    auto docId = blockData->value();
                    hits[docId]++;
				}
			}
		}
	nextBlock:
		block++;
	}
}
