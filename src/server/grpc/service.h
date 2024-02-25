#pragma once

#include <QSharedPointer>

#include "index/multi_index.h"
#include "server/grpc/proto/index.grpc.pb.h"
#include "server/metrics.h"

namespace Acoustid {
namespace Server {

class IndexServiceImpl final : public fpindex::Index::Service {
 public:
    IndexServiceImpl(QSharedPointer<MultiIndex> indexes, QSharedPointer<Metrics> metrics);

    virtual ::grpc::Status GetIndex(::grpc::ServerContext* context, const fpindex::GetIndexRequest* request,
                                    fpindex::GetIndexResponse* response) override;

    virtual ::grpc::Status CreateIndex(::grpc::ServerContext* context, const fpindex::CreateIndexRequest* request,
                                       fpindex::CreateIndexResponse* response) override;

    virtual ::grpc::Status DeleteIndex(::grpc::ServerContext* context, const fpindex::DeleteIndexRequest* request,
                                       fpindex::DeleteIndexResponse* response) override;

    virtual ::grpc::Status Update(::grpc::ServerContext* context, const fpindex::UpdateRequest* request,
                                  fpindex::UpdateResponse* response) override;

    virtual ::grpc::Status Search(::grpc::ServerContext* context, const fpindex::SearchRequest* request,
                                  fpindex::SearchResponse* response) override;

 private:
    QSharedPointer<MultiIndex> m_indexes;
    QSharedPointer<Metrics> m_metrics;
};

}  // namespace Server
}  // namespace Acoustid
