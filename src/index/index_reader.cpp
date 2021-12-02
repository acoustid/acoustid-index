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
#include "segment_docs.h"
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

bool IndexReader::containsDocument(uint32_t docId)
{
    auto currentInfo = std::make_pair<uint32_t, bool>(0, false);
    for (auto segment : m_info.segments()) {
        qDebug() << "Checking segment" << segment.id();
        auto update = segment.docs()->findDocumentUpdate(docId, currentInfo.first);
        if (update.has_value()) {
            qDebug() << "Segment" << segment.id() << "has update for doc" << docId;
            currentInfo = update.value();
        }
    }
    return currentInfo.first > 0 && !currentInfo.second;
}

std::vector<SearchResult> IndexReader::search(const std::vector<uint32_t> &terms, int64_t timeoutInMSecs)
{
    auto deadline = timeoutInMSecs > 0 ? (QDateTime::currentMSecsSinceEpoch() + timeoutInMSecs) : 0;
	const SegmentInfoList& segments = m_info.segments();

    auto sortedTerms = std::vector<uint32_t>(terms);
	std::sort(sortedTerms.begin(), sortedTerms.end());

    QHash<uint32_t, std::tuple<uint32_t, uint32_t, int>> hits;
    std::unordered_map<uint32_t, int> segmentHits;
	for (int i = 0; i < segments.size(); i++) {
        if (deadline > 0) {
            if (QDateTime::currentMSecsSinceEpoch() > deadline) {
                throw TimeoutExceeded();
            }
        }
		const SegmentInfo& s = segments.at(i);
		SegmentSearcher searcher(s.index(), segmentDataReader(s), s.lastKey());
        segmentHits.clear();
		searcher.search(sortedTerms.data(), sortedTerms.size(), segmentHits);
        auto segmentId = s.id();
        for (auto hit : segmentHits) {
            auto docId = hit.first;
            auto score = hit.second;
            auto version = s.docs()->getVersion(docId);
            auto it = hits.find(docId);
            if (it == hits.end()) {
                hits[docId] = std::make_tuple(score, version, segmentId);
            } else {
                if (version > std::get<1>(it.value())) {
                    it.value() = std::make_tuple(score, version, segmentId);
                }
            }
        }
	}

    std::vector<SearchResult> results;

    for (auto it = hits.begin(); it != hits.end(); ++it) {
        auto docId = it.key();
        auto score = std::get<0>(it.value());
        auto version = std::get<1>(it.value());
        auto segmentId = std::get<2>(it.value());
        auto currentVersion = version;
        for (auto segment : segments) {
            if (segment.id() != segmentId) {
                auto docInfo = segment.docs()->get(docId);
                currentVersion = std::max(currentVersion, docInfo.version());
            }
        }
        if (currentVersion == version) {
            results.push_back(SearchResult(docId, score));
        }
    }

    std::sort(results.begin(), results.end(), [](const SearchResult& a, const SearchResult& b) {
        return a.score() >= b.score();
    });

    return results;
}

