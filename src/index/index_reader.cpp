// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <algorithm>
#include <QDateTime>
#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_index_reader.h"
#include "segment_data_reader.h"
#include "segment_searcher.h"
#include "index.h"
#include "index_reader.h"

using namespace Acoustid;

IndexReader::IndexReader(DirectorySharedPtr dir, const IndexInfo& info)
	: m_dir(dir), m_info(info)
{
}

IndexReader::IndexReader(IndexSharedPtr index)
	: m_dir(index->directory()), m_index(index)
{
	m_info = m_index->acquireInfo();
}

IndexReader::~IndexReader()
{
	if (m_index) {
		m_index->releaseInfo(m_info);
	}
}

SegmentDataReader* IndexReader::segmentDataReader(const SegmentInfo& segment)
{
	return new SegmentDataReader(m_dir->openFile(segment.dataFileName()), BLOCK_SIZE);
}

void IndexReader::search(const uint32_t* fingerprint, size_t length, Collector* collector, int64_t timeoutInMSecs)
{
    auto deadline = timeoutInMSecs > 0 ? (QDateTime::currentMSecsSinceEpoch() + timeoutInMSecs) : 0;
    std::vector<uint32_t> fp(fingerprint, fingerprint + length);
	std::sort(fp.begin(), fp.end());
	const SegmentInfoList& segments = m_info.segments();
	for (int i = 0; i < segments.size(); i++) {
        if (deadline > 0) {
            if (QDateTime::currentMSecsSinceEpoch() > deadline) {
                throw TimeoutExceeded();
            }
        }
		const SegmentInfo& s = segments.at(i);
		SegmentSearcher searcher(s.index(), segmentDataReader(s), s.lastKey());
		searcher.search(fp.data(), fp.size(), collector);
	}
}

