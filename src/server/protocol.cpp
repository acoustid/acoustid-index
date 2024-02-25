#include "protocol.h"
#include "session.h"
#include "errors.h"

namespace Acoustid { namespace Server {

QVector<uint32_t> parseFingerprint(const QString &input) {
	QStringList inputParts = input.split(',');
    QVector<uint32_t> output;
    output.reserve(inputParts.size());
    for (int i = 0; i < inputParts.size(); i++) {
        bool ok;
        auto value = inputParts.at(i).toInt(&ok);
        if (!ok) {
            throw HandlerException("invalid fingerprint");
        }
        output.append(value);
    }
    if (output.isEmpty()) {
        throw HandlerException("empty fingerprint");
    }
    return output;
}

QString renderResponse(const QString &response) {
    return QString("OK %1").arg(response);
}

QString renderErrorResponse(const QString &response) {
    return QString("ERR %1").arg(response);
}

QSharedPointer<Request> parseRequest(const QString &line) {
    auto args = line.split(' ');
    if (args.size() < 1) {
        throw BadRequest("missing command");
    }
    auto command = args.takeFirst();
    return QSharedPointer<Request>::create(command, args);
}

ScopedHandlerFunc buildHandler(const QString &command, const QStringList &args) {
    if (command == "echo") {
        return [=](QSharedPointer<Session>) { return args.join(" "); };
    } else if (command == "get") {
        if (args.size() != 1) {
            if (args.size() == 2 and args.at(0) == "attribute") {  // backwards compatibility
                return [=](QSharedPointer<Session> session) { return session->getAttribute(args.at(1)); };
            } else {
                throw BadRequest("expected one argument");
            }
        } else {
            return [=](QSharedPointer<Session> session) { return session->getAttribute(args.at(0)); };
        }
    } else if (command == "set") {
        if (args.size() != 2) {
            if (args.size() == 3 and args.at(0) == "attribute") {  // backwards compatibility
                return [=](QSharedPointer<Session> session) { session->setAttribute(args.at(1), args.at(2)); return QString(); };
            } else {
                throw BadRequest("expected two arguments");
            }
        } else {
            return [=](QSharedPointer<Session> session) { session->setAttribute(args.at(0), args.at(1)); return QString(); };
        }
    } else if (command == "begin") {
        return [=](QSharedPointer<Session> session) { session->begin(); return QString(); };
    } else if (command == "commit") {
        return [=](QSharedPointer<Session> session) { session->commit(); session->clearTraceId(); return QString(); };
    } else if (command == "rollback") {
        return [=](QSharedPointer<Session> session) { session->rollback(); session->clearTraceId(); return QString(); };
    } else if (command == "optimize") {
        return [=](QSharedPointer<Session> session) { session->optimize(); session->clearTraceId(); return QString(); };
    } else if (command == "cleanup") {
        return [=](QSharedPointer<Session> session) { session->cleanup(); session->clearTraceId(); return QString(); };
    } else if (command == "insert") {
        if (args.size() != 2) {
            throw BadRequest("expected two arguments");
        }
        return [=](QSharedPointer<Session> session) {
            auto id = args.at(0).toInt();
            auto hashes = parseFingerprint(args.at(1));
            session->insert(id, hashes);
	    session->clearTraceId();
            return QString();
        };
    } else if (command == "search") {
        if (args.size() != 1) {
            throw BadRequest("expected one argumemt");
        }
        return [=](QSharedPointer<Session> session) {
            auto hashes = parseFingerprint(args.at(0));
            auto results = session->search(hashes);
            QStringList output;
            output.reserve(results.size());
            for (int i = 0; i < results.size(); i++) {
                output.append(QString("%1:%2").arg(results[i].id()).arg(results[i].score()));
            }
	    QString outputString = output.join(" ");
	    session->clearTraceId();
            return outputString;
        };
    } else {
        throw BadRequest(QString("unknown command %1").arg(command));
    }
}

HandlerFunc injectSessionIntoHandler(QWeakPointer<Session> session, ScopedHandlerFunc handler) {
    return [=]() {
        auto lockedSession = session.lock();
        if (!lockedSession) {
            throw HandlerException("session expired");
        }
        return handler(lockedSession);
    };
}

} // namespace Server
} // namespace Acoustid
