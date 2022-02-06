#include "service.h"

namespace Acoustid {
namespace Server {

IndexServiceImpl::IndexServiceImpl(QSharedPointer<MultiIndex> indexes, QSharedPointer<Metrics> metrics) : m_indexes(indexes), m_metrics(metrics)
{
}

} // namespace Server
} // namespace Acoustid
