// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SEGMENT_INFO_LIST_H_
#define ACOUSTID_SEGMENT_INFO_LIST_H_

#include <QList>
#include <QStringList>
#include <QSharedData>
#include <QSharedDataPointer>
#include <algorithm>
#include "common.h"
#include "segment_info.h"

namespace Acoustid {

class Directory;
class InputStream;
class OutputStream;

// Internal, do not use.
class IndexInfoData : public QSharedData
{
public:
	IndexInfoData() : nextSegmentNum(0), revision(-1) { }
	IndexInfoData(const IndexInfoData& other)
		: QSharedData(other),
		segments(other.segments),
		nextSegmentNum(other.nextSegmentNum),
		revision(other.revision),
		attribs(other.attribs) { }
	~IndexInfoData() { }

	SegmentInfoList segments;
	size_t nextSegmentNum;
	int revision;
	QMap<QString, QString> attribs;
};

// Information about the index structure. Implicitly shared, can be very
// efficiently copied.
class IndexInfo
{
public:
	IndexInfo() : d(new IndexInfoData()) {}

	// Return the current index revision
	int revision() const
	{
		return d->revision;
	}

	// Set the index revision
	void setRevision(int revision)
	{
		d->revision = revision;
	}

	// Increment the index revision by one and return it
	int incRevision()
	{
		return ++d->revision;
	}

	size_t segmentCount() const
	{
		return d->segments.size();
	}

	const SegmentInfo& segment(int idx) const
	{
		return d->segments.at(idx);
	}

	const SegmentInfoList& segments() const
	{
		return d->segments;
	}

	SegmentInfoList& segments()
	{
		return d->segments;
	}

	void setSegments(const SegmentInfoList& segments)
	{
		d->segments = segments;
	}

	void clearSegments()
	{
		d->segments.clear();
	}

	void addSegment(const SegmentInfo& info)
	{
		d->segments.append(info);
	}

	size_t lastSegmentId() const
	{
		return d->nextSegmentNum;
	}

	size_t incLastSegmentId()
	{
		return d->nextSegmentNum++;
	}

	void setLastSegmentId(size_t n)
	{
		d->nextSegmentNum = n;
	}

	const QMap<QString, QString>& attributes() const
	{
		return d->attribs;
	}

	QMap<QString, QString>& attributes()
	{
		return d->attribs;
	}

    bool hasAttribute(const QString& name) const
	{
		return d->attribs.contains(name);
	}

	QString getAttribute(const QString& name) const
	{
		return d->attribs.value(name);
	}

	void setAttribute(const QString& name, const QString& value)
	{
		d->attribs.insert(name, value);
	}

	QList<QString> files(bool includeIndexInfo = true) const;

	// Load the latest index info from a directory
	bool load(Directory* dir, bool loadIndexes = false);

	// Save a new index info revision into a directory
	void save(Directory* dir);

	// Find the last index info revision in a directory, returns -1 if
	// there is no index info file
	static int findCurrentRevision(Directory *dir, int maxRevision = 0);

	// Generate the index info file name
	static QString indexInfoFileName(int revision);

	// Extract the revision number from a index info file name
	static int indexInfoRevision(const QString &fileName);

private:

	// Load the index info from a specific file
	void load(InputStream* input, bool loadIndexes, Directory* dir);

	// Save the index info into a specific file
	void save(OutputStream* output);

	QSharedDataPointer<IndexInfoData> d;
};

}

#endif

