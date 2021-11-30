// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_REQUEST_H_
#define ACOUSTID_SERVER_REQUEST_H_

#include <QElapsedTimer>
#include <QString>
#include <QStringList>

namespace Acoustid {
namespace Server {

class Request {
 public:
    Request(const QString &command, const QStringList &args);
    ~Request();

    QString command() const { return m_command; }
    QStringList args() const { return m_args; }

    uint64_t elapsed() const { return m_timer.elapsed(); }

 private:
    QString m_command;
    QStringList m_args;
    QElapsedTimer m_timer;
};

}  // namespace Server
}  // namespace Acoustid

#endif
