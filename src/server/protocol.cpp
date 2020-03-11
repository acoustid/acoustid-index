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

HandlerFunc wrapHandlerFunc(HandlerFunc func) {
    return [=]() {
        try {
            auto result = func();
            return renderResponse(result);
        }
        catch (const HandlerException &ex) {
            return renderErrorResponse(ex.what());
        }
        catch (const Exception &ex) {
            qCritical() << "Unexpected exception in handler" << ex.what();
            return renderErrorResponse(ex.what());
        }
    };
}

HandlerFunc buildHandler(QSharedPointer<Session> session, const QString &line) {
    auto args = line.split(' ');
    if (args.size() < 1) {
        throw HandlerException("missing command");
    }
    auto command = args.takeFirst();
    if (command == "quit") {
        throw CloseRequested();
    }
    if (command == "echo") {
        return [=]() { return args.join(" "); };
    }
    if (command == "get") {
        if (args.size() != 1) {
            throw HandlerException("expected one argument");
        }
        return [=]() { return session->getAttribute(args.at(0)); };
    }
    if (command == "set") {
        if (args.size() != 2) {
            throw HandlerException("expected two arguments");
        }
        return [=]() { session->setAttribute(args.at(0), args.at(1)); return QString(); };
    }
    if (command == "begin") {
        return [=]() { session->begin(); return QString(); };
    }
    if (command == "commit") {
        return [=]() { session->commit(); return QString(); };
    }
    if (command == "rollback") {
        return [=]() { session->rollback(); return QString(); };
    }
    if (command == "optimize") {
        return [=]() { session->optimize(); return QString(); };
    }
    if (command == "cleanup") {
        return [=]() { session->cleanup(); return QString(); };
    }
    if (command == "cleanup") {
        return [=]() { session->cleanup(); return QString(); };
    }
    if (command == "insert") {
        return [=]() {
            if (args.size() != 2) {
                throw HandlerException("expected two arguments");
            }
            auto id = args.at(0).toInt();
            auto hashes = parseFingerprint(args.at(1));
            session->insert(id, hashes);
            return QString();
        };
    }
    if (command == "search") {
        return [=]() {
            if (args.size() != 1) {
                throw HandlerException("expected one argumemt");
            }
            auto hashes = parseFingerprint(args.at(0));
            auto results = session->search(hashes);
            QStringList output;
            output.reserve(results.size());
            for (int i = 0; i < results.size(); i++) {
                output.append(QString("%1:%2").arg(results[i].id()).arg(results[i].score()));
            }
            return output.join(" ");
        };
    }
    throw HandlerException("unknown command");
}

} // namespace Server
} // namespace Acoustid
