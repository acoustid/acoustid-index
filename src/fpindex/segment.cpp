#include "fpindex/segment.h"

#include "fpindex/proto/internal.pb.h"
#include "fpindex/search_result.h"

namespace fpindex {

void Segment::Search(const std::vector<uint32_t>& hashes, std::vector<SearchResult>* results) { results->clear(); }

}  // namespace fpindex
