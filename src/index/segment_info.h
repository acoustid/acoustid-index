// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SEGMENT_INFO_H_
#define ACOUSTID_SEGMENT_INFO_H_

#include <QSharedData>
#include <QSharedDataPointer>
#include "common.h"

namespace Acoustid {

// Internal, do not use.
class SegmentInfoData : public QSharedData
{
public:
	SegmentInfoData(int id = 0, size_t blockCount = 0, uint32_t lastKey = 0) :
		id(id),
		blockCount(blockCount),
		lastKey(lastKey) { }
	SegmentInfoData(const SegmentInfoData& other) :
		QSharedData(other),
		id(other.id),
		blockCount(other.blockCount),
		lastKey(other.lastKey) { }
	~SegmentInfoData() { }

	int id;
	size_t blockCount;
	uint32_t lastKey;
};

class SegmentInfo
{
public:
	SegmentInfo(int id = 0, size_t blockCount = 0, uint32_t lastKey = 0)
		: d(new SegmentInfoData(id, blockCount, lastKey)) {	}

	QString name() const
	{
		return QString("segment_%1").arg(id());
	}

	QString indexFileName() const
	{
		return name() + ".fii";
	}

	QString dataFileName() const
	{
		return name() + ".fid";
	}

	void setId(int id)
	{
		d->id = id;
	}

	int id() const
	{
		return d->id;
	}

	uint32_t lastKey() const
	{
		return d->lastKey;
	}

	void setLastKey(uint32_t lastKey)
	{
		d->lastKey = lastKey;
	}

	size_t blockCount() const
	{
		return d->blockCount;
	}

	void setBlockCount(size_t blockCount)
	{
		d->blockCount = blockCount;
	}

	QList<QString> files() const;

private:
	QSharedDataPointer<SegmentInfoData> d;
};

typedef QList<SegmentInfo> SegmentInfoList;

}

#endif
