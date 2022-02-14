#pragma once

#include <QSharedPointer>

#include "index/multi_index.h"
#include "server/grpc/proto/index.grpc.pb.h"
#include "server/metrics.h"

namespace Acoustid {
namespace Server {

class IndexServiceImpl final : public PB::Index::Service {
 public:
    IndexServiceImpl(QSharedPointer<MultiIndex> indexes, QSharedPointer<Metrics> metrics);

    virtual ::grpc::Status GetIndex(::grpc::ServerContext* context, const PB::GetIndexRequest* request,
                                    PB::GetIndexResponse* response) override;

    virtual ::grpc::Status CreateIndex(::grpc::ServerContext* context, const PB::CreateIndexRequest* request,
                                       PB::CreateIndexResponse* response) override;

    virtual ::grpc::Status DeleteIndex(::grpc::ServerContext* context, const PB::DeleteIndexRequest* request,
                                       PB::DeleteIndexResponse* response) override;

    virtual ::grpc::Status Update(::grpc::ServerContext* context, const PB::UpdateRequest* request,
                                  PB::UpdateResponse* response) override;

    virtual ::grpc::Status Search(::grpc::ServerContext* context, const PB::SearchRequest* request,
                                  PB::SearchResponse* response) override;

 private:
    QSharedPointer<MultiIndex> m_indexes;
    QSharedPointer<Metrics> m_metrics;
};

}  // namespace Server
}  // namespace Acoustid
