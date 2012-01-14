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
	SearchHandler(Connection* connection, const QStringList& args, int maxResults = 500, int topScorePercent = 10)
		: Handler(connection, args), m_topScorePercent(topScorePercent), m_maxResults(maxResults) { }

	void setTopScorePercent(int v)
	{
		m_topScorePercent = v;
	}

	void setMaxResults(int v)
	{
		m_maxResults = v;
	}

	QString handle()
	{
		if (args().size() < 1) {
			throw HandlerException("expected 1 argument");
		}
		QStringList fingerprint = args().first().split(',');
		ScopedArrayPtr<int32_t> fp(new int32_t[fingerprint.size()]);
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
		TopHitsCollector collector(m_maxResults, m_topScorePercent);
		ScopedPtr<IndexReader> reader(new IndexReader(index()));
		reader->search(reinterpret_cast<uint32_t*>(fp.get()), fpsize, &collector);
		QList<Result> results = collector.topResults();
		QStringList output;
		for (int j = 0; j < results.size(); j++) {
			output.append(QString("%1:%2").arg(results[j].id()).arg(results[j].score()));
		}
		return output.join(" ");
	}

private:
	int m_topScorePercent;
	int m_maxResults;
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
		IndexWriterSharedPtr writer(new IndexWriter(index()));
		connection()->setIndexWriter(writer);
		return QString();
	}
};

class CommitHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(CommitHandler)

	QString handle()
	{
		IndexWriterSharedPtr writer = connection()->indexWriter();
		if (!writer) {
			throw HandlerException("not in transaction");
		}
		writer->commit();
		connection()->setIndexWriter(IndexWriterSharedPtr());
		return QString();
	}
};

class RollbackHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(RollbackHandler)

	QString handle()
	{
		IndexWriterSharedPtr writer = connection()->indexWriter();
		if (!writer) {
			throw HandlerException("not in transaction");
		}
		connection()->setIndexWriter(IndexWriterSharedPtr());
		return QString();
	}
};

class InsertHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(InsertHandler)

	QString handle()
	{
		IndexWriterSharedPtr writer = connection()->indexWriter();
		if (!writer) {
			throw HandlerException("not in transaction");
		}
		if (args().size() < 2) {
			throw HandlerException("expected 2 arguments");
		}
		int32_t id = args().at(0).toInt();
		QStringList fingerprint = args().at(1).split(',');
		ScopedArrayPtr<int32_t> fp(new int32_t[fingerprint.size()]);
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
		writer->addDocument(id, reinterpret_cast<uint32_t*>(fp.get()), fpsize);
		return QString();
	}
};

class SetAttributeHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(SetAttributeHandler)

	QString handle()
	{
		IndexWriterSharedPtr writer = connection()->indexWriter();
		if (!writer) {
			throw HandlerException("not in transaction");
		}
		writer->setAttribute(args().at(1), args().at(2));
		return QString();
	}
};

class GetAttributeHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(GetAttributeHandler)

	QString handle()
	{
		IndexWriterSharedPtr writer = connection()->indexWriter();
		if (writer) {
			return writer->info().attribute(args().at(1));
		}
		return index()->info().attribute(args().at(1));
	}
};

class CleanupHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(CleanupHandler)

	QString handle()
	{
		IndexWriterSharedPtr writer = connection()->indexWriter();
		if (!writer) {
			throw HandlerException("not in transaction");
		}
		writer->cleanup();
		return QString();
	}
};

class OptimizeHandler : public Handler
{
public:
	ACOUSTID_HANDLER_CONSTRUCTOR(OptimizeHandler)

	QString handle()
	{
		IndexWriterSharedPtr writer = connection()->indexWriter();
		if (!writer) {
			throw HandlerException("not in transaction");
		}
		writer->optimize();
		return QString();
	}
};

}
}

#endif

