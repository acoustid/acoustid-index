// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_DOCUMENT_H_
#define ACOUSTID_INDEX_DOCUMENT_H_

#include <QVector>
#include "common.h"

namespace Acoustid {

typedef QVector<uint32_t> Document;

inline Document makeDocument(const uint32_t *data, size_t length)
{
	Document doc(length);
	qCopy(data, data + length, doc.begin());
	return doc;
}

}

#endif

