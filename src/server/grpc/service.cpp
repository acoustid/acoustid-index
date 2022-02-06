#include "service.h"

namespace Acoustid {
namespace Server {

IndexServiceImpl::IndexServiceImpl(QSharedPointer<MultiIndex> indexes, QSharedPointer<Metrics> metrics)
    : m_indexes(indexes), m_metrics(metrics) {}

static inline int remainingTime(std::chrono::system_clock::time_point deadline) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::system_clock::now()).count();
}

grpc::Status IndexServiceImpl::Search(grpc::ServerContext* context, const PB::SearchRequest* request,
                                      PB::SearchResponse* response) {
    auto indexName = QString::fromStdString(request->index_name());
    std::vector<uint32_t> terms;
    terms.assign(request->terms().begin(), request->terms().end());
    try {
        auto index = m_indexes->getIndex(indexName);
        auto results = index->search(terms, remainingTime(context->deadline()));
        for (auto result : results) {
            if (response->results_size() >= request->max_results()) {
                break;
            }
            auto r = response->add_results();
            r->set_doc_id(result.docId());
            r->set_score(result.score());
        }
    } catch (const IndexNotFoundException& e) {
        return grpc::Status(grpc::NOT_FOUND, e.what());
    }
    return grpc::Status::OK;
}

}  // namespace Server
}  // namespace Acoustid
