// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_COLLECTOR_H_
#define ACOUSTID_INDEX_COLLECTOR_H_

#include "common.h"

namespace Acoustid {

class Collector
{
public:
	virtual ~Collector() {}
	virtual void collect(uint32_t id) = 0;
};

}

#endif

