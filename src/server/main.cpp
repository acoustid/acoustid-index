// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <QCoreApplication>
#include <QThreadPool>
#include <QJsonObject>
#include <QJsonDocument>

#include "http.h"
#include "index/index.h"
#include "index/multi_index.h"
#include "listener.h"
#include "metrics.h"
#include "qhttpserver.hpp"
#include "qhttpserverrequest.hpp"
#include "qhttpserverresponse.hpp"
#include "server/grpc/service.h"
#include "store/fs_directory.h"
#include "util/options.h"

using namespace Acoustid;
using namespace Acoustid::Server;

using namespace qhttp::server;

static QTextStream stderrStream(stderr);

void handleLogMessage(QtMsgType type, const QMessageLogContext &context, const QString &msg)
{
    QString time = QDateTime::currentDateTimeUtc().toString(Qt::ISODateWithMs);

    QString level;
    switch (type) {
	case QtDebugMsg:
	    level = "debug";
	    break;
	case QtInfoMsg:
	    level = "info";
	    break;
	case QtWarningMsg:
	    level = "warning";
	    break;
	case QtCriticalMsg:
	    level = "error";
	    break;
	case QtFatalMsg:
	    level = "error";
	    break;
    }

    QJsonObject obj;
    obj.insert("time", time);
    obj.insert("level", level);
    obj.insert("message", msg);

    stderrStream << QJsonDocument(obj).toJson(QJsonDocument::Compact) << Qt::endl;

    if (type == QtFatalMsg) {
	abort();
    }
}

int main(int argc, char **argv)
{
    qInstallMessageHandler(handleLogMessage);

    OptionParser parser("%prog [options]");

    // clang-format off

    parser.addOption("directory", 'd')
        .setArgument()
        .setHelp("index directory")
        .setMetaVar("DIR")
        .setDefaultValue(".");

    parser.addOption("address", 'a')
        .setArgument()
        .setHelp("listen on this address (default: 127.0.0.1)")
        .setDefaultValue("127.0.0.1");

    parser.addOption("port", 'p')
        .setArgument()
        .setHelp("listen on this port (default: 6080)")
        .setDefaultValue("6080");

    parser.addOption("http-address")
        .setArgument()
        .setHelp("http server listens on this address (default: 127.0.0.1)")
        .setMetaVar("ADDRESS")
        .setDefaultValue("127.0.0.1");

    parser.addOption("http-port")
        .setArgument()
        .setHelp("http server listens on this port (default: 6081)")
        .setMetaVar("PORT")
        .setDefaultValue("6081");

    parser.addOption("grpc-address")
        .setArgument()
        .setHelp("gRPC server listens on this address (default: 127.0.0.1)")
        .setMetaVar("ADDRESS")
        .setDefaultValue("127.0.0.1");

    parser.addOption("grpc-port")
        .setArgument()
        .setHelp("gRPC server listens on this port (default: 6082)")
        .setMetaVar("PORT")
        .setDefaultValue("6082");

    // clang-format on

    std::unique_ptr<Options> opts(parser.parse(argc, argv));

    QString path = opts->option("directory");

    QString address = opts->option("address");
    int port = opts->option("port").toInt();

    QString httpAddress = opts->option("http-address");
    int httpPort = opts->option("http-port").toInt();

    auto grpcAddress = opts->option("grpc-address");
    auto grpcPort = opts->option("grpc-port").toInt();
    auto grpcEndpoint = QString("%1:%2").arg(grpcAddress).arg(grpcPort);

    QCoreApplication app(argc, argv);

    int numThreads = opts->option("threads").toInt();
    if (numThreads) {
        QThreadPool::globalInstance()->setMaxThreadCount(numThreads);
    }

    auto indexesDir = QSharedPointer<FSDirectory>::create(path, true);
    auto indexes = QSharedPointer<MultiIndex>::create(indexesDir);
    auto metrics = QSharedPointer<Metrics>::create();

    Listener::setupSignalHandlers();

    auto listener = QSharedPointer<Listener>::create(indexes->getRootIndex(true), metrics);
    listener->listen(QHostAddress(address), port);
    qDebug() << "Telnet server listening on" << address << "port" << port;

    auto httpHandler = QSharedPointer<HttpRequestHandler>::create(indexes, metrics);
    auto httpListener = QSharedPointer<QHttpServer>::create(&app);
    httpListener->listen(QHostAddress(httpAddress), httpPort, [=](auto req, auto res) {
        httpHandler->router().handle(req, res);
    });
    qDebug() << "HTTP server listening on" << httpAddress << "port" << httpPort;

    IndexServiceImpl service(indexes, metrics);

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder grpcServerBuilder;
    grpcServerBuilder.AddListeningPort(grpcEndpoint.toStdString(), grpc::InsecureServerCredentials());
    grpcServerBuilder.RegisterService(&service);
    qDebug() << "Starting gRPC server at" << grpcAddress << "port" << grpcPort;
    auto grpcServer = grpcServerBuilder.BuildAndStart();

    auto exitCode = app.exec();

    qDebug() << "Stopping gRPC server";
    grpcServer->Shutdown();

    return exitCode;
}
