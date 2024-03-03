#pragma once

#include <QDebug>

#define LOG_ERROR() qCritical()
#define LOG_WARNING() qWarning()
#define LOG_INFO() qInfo()
#define LOG_DEBUG() qDebug()
