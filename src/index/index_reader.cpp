// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <algorithm>
#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_index_reader.h"
#include "segment_data_reader.h"
#include "segment_merger.h"
#include "index_reader.h"

#define BLOCK_SIZE 512

using namespace Acoustid;

IndexReader::IndexReader(Directory *dir)
	: m_dir(dir), m_revision(-1)
{
}

void IndexReader::open()
{
	m_revision = SegmentInfoList::findCurrentRevision(m_dir);
	if (m_revision != -1) {
		m_infos.read(m_dir->openFile(SegmentInfoList::segmentsFileName(m_revision)));
	}
	else {
		throw IOException("there is no index in the directory");
	}
}

IndexReader::~IndexReader()
{
}

