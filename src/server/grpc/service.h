#pragma once

#include <QSharedPointer>
#include "index/multi_index.h"
#include "server/metrics.h"
#include "server/grpc/proto/index.grpc.pb.h"

namespace Acoustid {
namespace Server {

class IndexServiceImpl final : public PB::Index::Service {
 public:
    IndexServiceImpl(QSharedPointer<MultiIndex> indexes, QSharedPointer<Metrics> metrics);

 private:
    QSharedPointer<MultiIndex> m_indexes;
    QSharedPointer<Metrics> m_metrics;
};

} // namespace Server
} // namespace Acoustid
