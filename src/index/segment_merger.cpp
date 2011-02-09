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

#include "segment_merger.h"

using namespace Acoustid;

SegmentMerger::SegmentMerger(SegmentDataWriter *writer)
	: m_writer(writer)
{
}

void SegmentMerger::merge()
{
	QMutableListIterator<SegmentEnum *> iter(m_readers);
	while (iter.hasNext()) {
		SegmentEnum *reader = iter.next();
		if (!reader->next()) {
			iter.remove();
		}
	}
	uint64_t lastMinItem = ~0;
	while (!m_readers.isEmpty()) {
		size_t minItemIndex = 0;
		uint64_t minItem = ~0;
		for (size_t i = 0; i < m_readers.size(); i++) {
			SegmentEnum *reader = m_readers[i];
			uint64_t item = (uint64_t(reader->key()) << 32) | reader->value();
			if (item < minItem) {
				minItem = item;
				minItemIndex = i;
			}
		}
		if (!m_readers[minItemIndex]->next()) {
			m_readers.removeAt(minItemIndex);
		}
		if (minItem != lastMinItem) {
			m_writer->addItem((minItem >> 32) & 0xffffffff, minItem & 0xffffffff);
			lastMinItem = minItem;
		}
	}
	m_writer->close();
}
