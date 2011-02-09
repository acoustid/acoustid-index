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
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_index_writer.h"
#include "segment_data_writer.h"
#include "index_writer.h"

using namespace Acoustid;

IndexWriter::IndexWriter(Directory *dir, bool create)
	: m_dir(dir), m_numDocsInBuffer(0), m_maxSegmentBufferSize(1024 * 1024 * 10)
{
	m_revision = SegmentInfoList::findCurrentRevision(dir);
	if (m_revision != -1) {
		m_segmentInfos.read(dir->openFile(SegmentInfoList::segmentsFileName(m_revision)));
	}
	else if (create) {
		commit();
	}
	else {
		throw IOException("there is no index in the directory");
	}
}

IndexWriter::~IndexWriter()
{
}

int IndexWriter::revision()
{
	return m_revision;
}

void IndexWriter::addDocument(uint32_t id, uint32_t *terms, size_t length)
{
	for (size_t i = 0; i < length; i++) {
		m_segmentBuffer.push_back((uint64_t(terms[i]) << 32) | id);
	}
	m_numDocsInBuffer++;
	maybeFlush();
}

void IndexWriter::commit()
{
	flush();
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

	QString name = "segment_0";
	ScopedPtr<OutputStream> indexOutput(m_dir->createFile(name + ".fii"));
	ScopedPtr<OutputStream> dataOutput(m_dir->createFile(name + ".fid"));

	SegmentIndexWriter indexWriter(indexOutput.get());
	indexWriter.setBlockSize(512);

	SegmentDataWriter writer(dataOutput.get(), &indexWriter, indexWriter.blockSize());
	for (size_t i = 0; i < m_segmentBuffer.size(); i++) {
		uint32_t key = (m_segmentBuffer[i] >> 32);
		uint32_t value = m_segmentBuffer[i] & 0xffffffff;
		writer.addItem(key, value);
	}
	writer.close();

	m_segmentInfos.add(SegmentInfo(name, m_numDocsInBuffer));

	m_segmentBuffer.clear();
	m_numDocsInBuffer = 0;
}

