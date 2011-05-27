// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "common.h"
#include "handler.h"

using namespace Acoustid;
using namespace Acoustid::Server;

Handler::Handler(Connection* connection, const QStringList& args)
	: m_connection(connection), m_args(args)
{
}

Handler::~Handler()
{
}

void Handler::run()
{
	QString result;
	try {
		result = QString("OK %1").arg(handle());
	}
	catch (HandlerException& ex) {
		result = QString("ERR %1").arg(ex.what());
	}
	catch (Exception& ex) {
		qCritical() << "Unexpected exception in handler" << ex.what();
		result = QString("ERR %1").arg(ex.what());
	}
	emit finished(result);
}

