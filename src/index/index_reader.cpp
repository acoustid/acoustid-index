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

std::unique_ptr<SegmentDataReader> IndexReader::segmentDataReader(const SegmentInfo& segment)
{
	return std::make_unique<SegmentDataReader>(m_dir->openFile(segment.dataFileName()), BLOCK_SIZE);
}

std::vector<SearchResult> IndexReader::search(const std::vector<uint32_t> &hashesIn, int64_t timeoutInMSecs)
{
    auto deadline = timeoutInMSecs > 0 ? (QDateTime::currentMSecsSinceEpoch() + timeoutInMSecs) : 0;

    std::vector<uint32_t> hashes(hashesIn);
    std::sort(hashes.begin(), hashes.end());

    std::unordered_map<uint32_t, int> hits;

    const SegmentInfoList& segments = m_info.segments();
    for (auto segment : segments) {
	if (deadline > 0) {
	    if (QDateTime::currentMSecsSinceEpoch() > deadline) {
		throw TimeoutExceeded();
	    }
	}
	SegmentSearcher searcher(segment.index(), segmentDataReader(segment), segment.lastKey());
	searcher.search(hashes, hits);
    }

    std::vector<SearchResult> results;
    results.reserve(hits.size());
    for (const auto &hit : hits) {
	results.emplace_back(hit.first, hit.second);
    }

    sortSearchResults(results);

    return results;
}
