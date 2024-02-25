#include <QThreadStorage>
#include "util/tracing.h"

static QThreadStorage<QString> traceId;

void setTraceId(const QString &value)
{
    traceId.setLocalData(value);
}

QString getTraceId()
{
    return traceId.localData();
}
