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

#include <algorithm>
#include "store/directory.h"
#include "index_writer.h"

IndexWriter::IndexWriter(Directory *dir)
	: m_dir(dir)
{
	m_revision = SegmentInfoList::findCurrentRevision(dir);
	if (m_revision != -1) {
		m_segmentInfos.read(dir->openFile(SegmentInfoList::segmentsFileName(m_revision)));
	}
	else {
		commit();
	}
}

IndexWriter::~IndexWriter()
{
}

void IndexWriter::addDocument(uint32_t id, uint32_t *terms, size_t length)
{
	for (size_t i = 0; i < length; i++) {
		m_segmentBuffer.push_back((uint64_t(terms[i]) << 32) | id);
	}
	maybeFlush();
}

void IndexWriter::commit()
{
	m_revision++;
	m_segmentInfos.write(m_dir->createFile(SegmentInfoList::segmentsFileName(m_revision)));
}

void IndexWriter::maybeFlush()
{
	if (m_segmentBuffer.size() > m_maxSegmentBufferSize) {
		flush();
	}
}

void IndexWriter::flush()
{
	if (m_segmentBuffer.empty()) {
		return;
	}
	std::sort(m_segmentBuffer.begin(), m_segmentBuffer.end());
	/*for (size_t i = 0; i < m_segmentBuffer.size(); i++) {
		uint32_t key = (m_segmentBuffer[i] >> 32);
		uint32_t value = m_segmentBuffer[i] & 0xffffffff;
		writer->addItem(key, value);
	}*/
	m_segmentBuffer.clear();
}

