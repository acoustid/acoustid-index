// Acoustid Index -- Inverted index for audio fingerprints
// Copyright (C) 2011  Lukas Lalinsky
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#include "store/output_stream.h"
#include "collector.h"
#include "segment_index.h"
#include "segment_data_reader.h"
#include "segment_searcher.h"

SegmentSearcher::SegmentSearcher(SegmentIndex *index, SegmentDataReader *dataReader)
	: m_index(index), m_dataReader(dataReader)
{
}

SegmentSearcher::~SegmentSearcher()
{
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

