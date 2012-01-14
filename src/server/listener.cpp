// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <signal.h>
#include <sys/socket.h>
#include <syslog.h>
#include <QCoreApplication>
#include "store/fs_directory.h"
#include "listener.h"
#include "connection.h"

using namespace Acoustid;
using namespace Acoustid::Server;

int Listener::m_sigIntFd[2];
int Listener::m_sigTermFd[2];

Listener::Listener(const QString& path, bool mmap, QObject* parent)
	: QTcpServer(parent),
	  m_dir(new FSDirectory(path, mmap)),
	  m_index(new Index(m_dir, true))
{
	m_sigIntNotifier = new QSocketNotifier(m_sigIntFd[1], QSocketNotifier::Read, this);
	connect(m_sigIntNotifier, SIGNAL(activated(int)), this, SLOT(handleSigInt()));
	m_sigTermNotifier = new QSocketNotifier(m_sigTermFd[1], QSocketNotifier::Read, this);
	connect(m_sigTermNotifier, SIGNAL(activated(int)), this, SLOT(handleSigTerm()));
	connect(this, SIGNAL(newConnection()), SLOT(acceptNewConnection()));
}

Listener::~Listener()
{
}

void Listener::sigIntHandler(int signal)
{
	char tmp = 1;
	::write(m_sigIntFd[0], &tmp, sizeof(tmp));
}

void Listener::sigTermHandler(int signal)
{
	char tmp = 1;
	::write(m_sigTermFd[0], &tmp, sizeof(tmp));
}

static void syslogMessageHandler(QtMsgType type, const char *msg)
{
	switch (type) {
	case QtDebugMsg:
		syslog(LOG_DEBUG, "%s", msg);
		break;
	case QtWarningMsg:
		syslog(LOG_WARNING, "%s", msg);
		break;
	case QtCriticalMsg:
		syslog(LOG_CRIT, "%s", msg);
		break;
	case QtFatalMsg:
		syslog(LOG_CRIT, "%s", msg);
		abort();
	}
}

void Listener::setupLogging(bool syslog, const QString& facility)
{
	QMap<QString, int> facilities;
	facilities["auth"] = LOG_AUTH;
	facilities["authpriv"] = LOG_AUTHPRIV;
	facilities["cron"] = LOG_CRON;
	facilities["ftp"] = LOG_FTP;
	facilities["kern"] = LOG_KERN;
	facilities["lpr"] = LOG_LPR;
	facilities["mail"] = LOG_MAIL;
	facilities["news"] = LOG_NEWS;
	facilities["syslog"] = LOG_SYSLOG;
	facilities["user"] = LOG_USER;
	facilities["uucp"] = LOG_UUCP;
	facilities["local0"] = LOG_LOCAL0;
	facilities["local1"] = LOG_LOCAL1;
	facilities["local2"] = LOG_LOCAL2;
	facilities["local3"] = LOG_LOCAL3;
	facilities["local4"] = LOG_LOCAL4;
	facilities["local5"] = LOG_LOCAL5;
	facilities["local6"] = LOG_LOCAL6;
	facilities["local7"] = LOG_LOCAL7;
	if (syslog) {
		openlog("fpi-server", LOG_PID, facilities.value(facility.toLower(), LOG_USER));
		setlogmask(LOG_UPTO(LOG_DEBUG));
		qInstallMsgHandler(syslogMessageHandler);
	}
}

void Listener::setupSignalHandlers()
{
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, m_sigIntFd)) {
		qFatal("Couldn't create SIGINT socketpair");
	}
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, m_sigTermFd)) {
		qFatal("Couldn't create SIGTERM socketpair");
	}
	struct sigaction sigint;
	sigint.sa_handler = Listener::sigIntHandler;
	sigemptyset(&sigint.sa_mask);
	sigint.sa_flags = 0;
	sigint.sa_flags |= SA_RESTART;
	if (sigaction(SIGINT, &sigint, 0) > 0) {
		qFatal("Couldn't install SIGINT handler");
	}
	struct sigaction sigterm;
	sigterm.sa_handler = Listener::sigTermHandler;
	sigemptyset(&sigterm.sa_mask);
	sigterm.sa_flags = 0;
	sigterm.sa_flags |= SA_RESTART;
	if (sigaction(SIGTERM, &sigterm, 0) > 0) {
		qFatal("Couldn't install SIGTERM handler");
	}
}

void Listener::handleSigInt()
{
	m_sigIntNotifier->setEnabled(false);
	char tmp;
	::read(m_sigIntFd[1], &tmp, sizeof(tmp));
	qDebug() << "Received SIGINT, stopping";
	stop(); // XXX handle this more gracefully
	m_sigIntNotifier->setEnabled(true);
}

void Listener::handleSigTerm()
{
	m_sigTermNotifier->setEnabled(false);
	char tmp;
	::read(m_sigTermFd[1], &tmp, sizeof(tmp));
	qDebug() << "Received SIGTERM, stopping";
	stop();
	m_sigTermNotifier->setEnabled(true);
}

void Listener::stop()
{
	qDebug() << "Stopping the listener";
	close();
	if (m_connections.isEmpty()) {
		qApp->quit();
	}
	else {
		connect(this, SIGNAL(lastConnectionClosed()), qApp, SLOT(quit()));
		foreach (Connection* connection, m_connections) {
			connection->close();
		}
	}
}

void Listener::removeConnection(Connection *connection)
{
	m_connections.removeAll(connection);
	if (m_connections.isEmpty()) {
		emit lastConnectionClosed();
	}
}

void Listener::acceptNewConnection()
{
	QTcpSocket* socket = nextPendingConnection();
	Connection* connection = new Connection(m_index, socket, this);
	m_connections.append(connection);
	connect(connection, SIGNAL(closed(Connection *)), SLOT(removeConnection(Connection *)));
}

