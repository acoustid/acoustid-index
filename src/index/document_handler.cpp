// Copyright (C) 2013  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "document_handler.h"

using namespace Acoustid;

DocumentHandler::DocumentHandler()
{
}

DocumentHandler::~DocumentHandler()
{
}

Document DocumentHandler::extractQuery(const Document &doc)
{
	return doc;
}

bool DocumentHandler::canCompare() const
{
	return false;
}

float DocumentHandler::compare(const Document &doc1, const Document &doc2)
{
	return 0.0;
}

