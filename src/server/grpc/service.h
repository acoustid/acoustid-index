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

    virtual ::grpc::Status Search(::grpc::ServerContext* context, const ::Acoustid::Server::PB::SearchRequest* request,
                                  ::Acoustid::Server::PB::SearchResponse* response) override;

 private:
    QSharedPointer<MultiIndex> m_indexes;
    QSharedPointer<Metrics> m_metrics;
};

}  // namespace Server
}  // namespace Acoustid
