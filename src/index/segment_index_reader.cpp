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

#include "store/input_stream.h"
#include "segment_index.h"
#include "segment_index_reader.h"

SegmentIndexReader::SegmentIndexReader(InputStream *input)
	: m_input(input)
{
}

SegmentIndexReader::~SegmentIndexReader()
{
}

SegmentIndex *SegmentIndexReader::read()
{
	size_t blockSize = m_input->readInt32();
	size_t indexInterval = m_input->readInt32();
	size_t keyCount = m_input->readInt32();
	SegmentIndex *index = new SegmentIndex(blockSize, indexInterval, keyCount);
	uint32_t *keys = index->levelKeys(0), lastKey = 0;
	for (size_t i = 0; i < keyCount; i++) {
		lastKey += m_input->readVInt32();
		*keys++ = lastKey;
	}
	index->rebuild();
	return index;
}

