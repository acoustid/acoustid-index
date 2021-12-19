// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_SEARCH_RESULT_H_
#define ACOUSTID_INDEX_SEARCH_RESULT_H_

namespace Acoustid {

class SearchResult {
 public:
    SearchResult(uint32_t docId, int score, uint32_t version = 0) : m_docId(docId), m_score(score), m_version(version) {}

    uint32_t docId() const { return m_docId; }
    int score() const { return m_score; }
    uint32_t version() const { return m_version; }

    // bool operator==(const SearchResult &other) const { return m_docId == other.m_docId && m_score == other.m_score && m_version == other.m_version; }
    bool operator==(const SearchResult &other) const { return m_docId == other.m_docId && m_score == other.m_score; }
    bool operator!=(const SearchResult &other) const { return !operator==(other); }

 private:
    uint32_t m_docId;
    int m_score;
    uint32_t m_version;
};

inline void sortSearchResults(std::vector<SearchResult> &results)
{
    std::sort(results.begin(), results.end(), [](const SearchResult &a, const SearchResult &b) {
        if (a.score() > b.score()) {
            return true;
        } else if (a.score() < b.score()) {
            return false;
        } else {
            return a.docId() < b.docId();
        }
    });
}

inline void filterSearchResults(std::vector<SearchResult> &results, size_t limit = 0, int minScorePercent = 0) {
    if (results.empty()) {
        return;
    }
    if (limit <= 0) {
        limit = results.size();
    }
    int minScore = results[0].score() * minScorePercent / 100;
    if (results.size() > limit) {
        results.erase(results.begin() + limit, results.end());
    }
    for (auto it = results.begin(); it != results.end(); ++it) {
        if (it->score() < minScore) {
            results.erase(it, results.end());
            break;
        }
    }
}

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_SEARCH_RESULT_H_
