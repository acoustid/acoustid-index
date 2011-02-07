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
#include "segment_data_reader.h"

using namespace Acoustid;

SegmentDataReader::SegmentDataReader(InputStream *input, size_t blockSize)
	: m_input(input), m_blockSize(blockSize)
{
}

SegmentDataReader::~SegmentDataReader()
{
}

void SegmentDataReader::setBlockSize(size_t blockSize)
{
	m_blockSize = blockSize;
}

BlockDataIterator *SegmentDataReader::readBlock(size_t n, uint32_t key)
{
	m_input->seek(m_blockSize * n);
	size_t length = m_input->readVInt32();
	return new BlockDataIterator(m_input, length, key);
}
