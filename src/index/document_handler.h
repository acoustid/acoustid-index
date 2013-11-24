// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_DOCUMENT_HANDLER_H_
#define ACOUSTID_INDEX_DOCUMENT_HANDLER_H_

#include "common.h"
#include "document.h"

namespace Acoustid {

class DocumentHandler
{
public:
	DocumentHandler();
	virtual ~DocumentHandler();

	// Extract a part of the document that should be indexed.
	virtual Document extractQuery(const Document &doc);

	// Whether this handler can compare full documents. If not, the number
	// of matched items from the indexed query is used as a score.
	virtual bool canCompare() const;

	// Compare two full documents and return the score.
	virtual float compare(const Document &doc1, const Document &doc2);
};

}

#endif
