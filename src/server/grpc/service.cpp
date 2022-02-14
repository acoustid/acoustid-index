#include "service.h"

#include <sstream>

namespace Acoustid {
namespace Server {

IndexServiceImpl::IndexServiceImpl(QSharedPointer<MultiIndex> indexes, QSharedPointer<Metrics> metrics)
    : m_indexes(indexes), m_metrics(metrics) {}

static inline int remainingTime(std::chrono::system_clock::time_point deadline) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::system_clock::now()).count();
}

grpc::Status IndexServiceImpl::GetIndex(grpc::ServerContext* context, const PB::GetIndexRequest* request,
                                        PB::GetIndexResponse* response) {
    auto indexName = QString::fromStdString(request->index_name());
    qDebug() << "[gRPC] GetIndex " << indexName;
    try {
        m_indexes->getIndex(indexName);
    } catch (const IndexNotFoundException& e) {
        return grpc::Status(grpc::NOT_FOUND, e.what());
    }
    return grpc::Status::OK;
}

grpc::Status IndexServiceImpl::CreateIndex(grpc::ServerContext* context, const PB::CreateIndexRequest* request,
                                           PB::CreateIndexResponse* response) {
    auto indexName = QString::fromStdString(request->index_name());
    qDebug() << "[gRPC] CreateIndex " << indexName;
    m_indexes->createIndex(indexName);
    return grpc::Status::OK;
}

grpc::Status IndexServiceImpl::DeleteIndex(grpc::ServerContext* context, const PB::DeleteIndexRequest* request,
                                           PB::DeleteIndexResponse* response) {
    auto indexName = QString::fromStdString(request->index_name());
    qDebug() << "[gRPC] DeleteIndex " << indexName;
    m_indexes->deleteIndex(indexName);
    return grpc::Status::OK;
}

grpc::Status IndexServiceImpl::Update(grpc::ServerContext* context, const PB::UpdateRequest* request,
                                      PB::UpdateResponse* response) {
    auto indexName = QString::fromStdString(request->index_name());
    qDebug() << "[gRPC] Update " << indexName;
    OpBatch batch;
    for (const auto& op : request->ops()) {
        switch (op.op_case()) {
            case PB::Operation::kInsertOrUpdateDocument: {
                const auto& data = op.insert_or_update_document();
                auto docId = data.doc_id();
                auto terms = std::vector<uint32_t>(data.terms().begin(), data.terms().end());
                batch.insertOrUpdateDocument(docId, terms);
                break;
            }
            case PB::Operation::kDeleteDocument: {
                const auto& data = op.delete_document();
                auto docId = data.doc_id();
                batch.deleteDocument(docId);
                break;
            }
            case PB::Operation::kSetAttribute: {
                const auto& data = op.set_attribute();
                auto name = data.name();
                auto value = data.value();
                batch.setAttribute(name, value);
                break;
            }
            default: {
                std::stringstream ss;
                ss << "Unknown operation type: " << op.op_case();
                return grpc::Status(::grpc::INVALID_ARGUMENT, ss.str());
            }
        }
    }
    try {
        auto index = m_indexes->getIndex(indexName);
        index->applyUpdates(batch);
    } catch (const IndexNotFoundException& e) {
        return grpc::Status(grpc::NOT_FOUND, e.what());
    }
    return grpc::Status::OK;
}

grpc::Status IndexServiceImpl::Search(grpc::ServerContext* context, const PB::SearchRequest* request,
                                      PB::SearchResponse* response) {
    auto indexName = QString::fromStdString(request->index_name());
    qDebug() << "[gRPC] Search " << indexName;
    std::vector<uint32_t> terms;
    terms.assign(request->terms().begin(), request->terms().end());
    try {
        auto index = m_indexes->getIndex(indexName);
        auto results = index->search(terms, remainingTime(context->deadline()));
        for (auto result : results) {
            if (request->max_results() > 0 && response->results_size() >= request->max_results()) {
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
