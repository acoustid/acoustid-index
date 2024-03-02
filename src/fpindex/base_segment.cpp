#include "fpindex/base_segment.h"

#include "fpindex/search_result.h"

namespace fpindex {

std::vector<SearchResult> BaseSegment::Search(const std::vector<uint32_t>& hashes) {
    std::vector<SearchResult> results;
    if (Search(hashes, &results)) {
        return results;
    }
    return std::vector<SearchResult>();
}

}  // namespace fpindex
