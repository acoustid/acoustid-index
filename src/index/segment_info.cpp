// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "segment_info.h"

using namespace Acoustid;

QList<QString> SegmentInfo::files() const
{
	QList<QString> files;
	files.append(indexFileName());
	files.append(dataFileName());
	files.append(docsFileName());
	return files;
}
