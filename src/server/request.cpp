#include "request.h"

namespace Acoustid {
namespace Server {

Request::Request(const QString &command, const QStringList &args) : m_command(command), m_args(args) {
    m_timer.start();
}

Request::~Request() {
}

}
}
