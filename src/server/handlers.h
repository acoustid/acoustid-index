// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_HANDLERS_H_
#define ACOUSTID_SERVER_HANDLERS_H_

#include "handler.h"
#include "index/index_reader.h"
#include "index/top_hits_collector.h"

namespace Acoustid {
namespace Server {

class EchoHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(EchoHandler)

	QString handle()
	{
		return args().join(" ");
	}
};

class SearchHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(SearchHandler)

	QString handle()
	{
		if (args().size() < 1) {
			throw HandlerException("expected 1 argument");
		}
		QStringList fingerprint = args().first().split(',');
		int32_t *fp = new int32_t[fingerprint.size()];
		size_t fpsize = 0;
		for (int j = 0; j < fingerprint.size(); j++) {
			bool ok;
			fp[fpsize] = fingerprint.at(j).toInt(&ok);
			if (ok) {
				fpsize++;
			}
		}
		if (!fpsize) {
			throw HandlerException("empty fingerprint");
		}
		TopHitsCollector collector(10);
		ScopedPtr<IndexReader> reader(index()->createReader());
		reader->search((uint32_t *)fp, fpsize, &collector);
		QList<Result> results = collector.topResults();
		QStringList output;
		for (int j = 0; j < results.size(); j++) {
			output.append(QString("%1:%2").arg(results[j].id()).arg(results[j].score()));
		}
		return output.join(" ");
	}
};

class BeginHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(BeginHandler)

	QString handle()
	{
		if (connection()->indexWriter()) {
			throw HandlerException("already in transaction");
		}
		connection()->setIndexWriter(index()->createWriter());
		return QString();
	}
};

class CommitHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(CommitHandler)

	QString handle()
	{
		IndexWriter* writer = connection()->indexWriter();
		if (!writer) {
			throw HandlerException("not in transaction");
		}
		writer->commit();
		connection()->setIndexWriter(NULL);
		return QString();
	}
};

class InsertHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(InsertHandler)

	QString handle()
	{
		IndexWriter* writer = connection()->indexWriter();
		if (!writer) {
			throw HandlerException("not in transaction");
		}
		if (args().size() < 2) {
			throw HandlerException("expected 2 arguments");
		}
		int32_t id = args().at(0).toInt();
		QStringList fingerprint = args().at(1).split(',');
		int32_t *fp = new int32_t[fingerprint.size()];
		size_t fpsize = 0;
		for (int j = 0; j < fingerprint.size(); j++) {
			bool ok;
			fp[fpsize] = fingerprint.at(j).toInt(&ok);
			if (ok) {
				fpsize++;
			}
		}
		if (!fpsize) {
			throw HandlerException("empty fingerprint");
		}
		writer->addDocument(id, (uint32_t*)fp, fpsize);
		return QString();
	}
};

}
}

#endif

