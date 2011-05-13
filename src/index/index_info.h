// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SEGMENT_INFO_LIST_H_
#define ACOUSTID_SEGMENT_INFO_LIST_H_

#include <QList>
#include <QStringList>
#include <algorithm>
#include "common.h"
#include "segment_info.h"

namespace Acoustid {

class Directory;
class InputStream;
class OutputStream;

class IndexInfo
{
public:
	typedef QList<SegmentInfo>::const_iterator const_iterator;
	typedef QList<SegmentInfo>::iterator iterator;

	IndexInfo() : m_nextSegmentNum(0), m_revision(-1)
	{
	}

	IndexInfo(const IndexInfo &other)
		: m_nextSegmentNum(other.lastSegmentId()),
		  m_infos(other.infos()),
		  m_revision(other.revision())
	{
	}

	// Return the current index revision
	int revision() const
	{
		return m_revision;
	}

	// Set the index revision
	void setRevision(int revision)
	{
		m_revision = revision;
	}

	// Increment the index revision by one and return it
	int incRevision()
	{
		return ++m_revision;
	}

	iterator begin()
	{
		return m_infos.begin();
	}

	const_iterator begin() const
	{
		return m_infos.begin();
	}

	iterator end()
	{
		return m_infos.end();
	}

	const_iterator end() const
	{
		return m_infos.end();
	}

	size_t size() const 
	{
		return segmentCount();
	}

	size_t segmentCount() const
	{
		return m_infos.size();
	}

	const SegmentInfo &info(size_t i) const
	{
		return m_infos[i];
	}

	const QList<SegmentInfo> &infos() const
	{
		return m_infos;
	}

	size_t lastSegmentId() const
	{
		return m_nextSegmentNum;
	}

	size_t incLastSegmentId()
	{
		return m_nextSegmentNum++;
	}

	void setLastSegmentId(size_t n)
	{
		m_nextSegmentNum = n;
	}

	void clear();
	void add(const SegmentInfo &info);

	// Load the latest index info from a directory
	bool load(Directory* dir);

	// Load the index info from a specific file
	void load(InputStream* input);

	// Save a new index info revision into a directory
	void save(Directory* dir);

	// Save the index info into a specific file
	void save(OutputStream* output);

	// Find the last index info revision in a directory, returns -1 if
	// there is no index info file
	static int findCurrentRevision(Directory *dir);

	// Generate the index info file name
	static QString indexInfoFileName(int revision);

	// Extract the revision number from a index info file name
	static int indexInfoRevision(const QString &fileName);

private:
	QList<SegmentInfo> m_infos;
	size_t m_nextSegmentNum;
	int m_revision;
};

}

#endif

