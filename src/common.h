// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_COMMON_H_
#define ACOUSTID_COMMON_H_

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <QString>
#include <QDebug>
#include <QSharedPointer>

#include "util/exceptions.h"
#include "util/scoped_ptr.h"

namespace Acoustid {

// Some default configuration options
static const int MAX_SEGMENT_BUFFER_SIZE = 1024 * 1024 * 5;
static const int BLOCK_SIZE = 512;
static const int MAX_MERGE_AT_ONCE = 4;
static const int MAX_SEGMENTS_PER_TIER = 3;
static const int MAX_SEGMENT_BLOCKS = 2 * 1024 * 1024;
static const int FLOOR_SEGMENT_BLOCKS = 1024;

#define ACOUSTID_DISABLE_COPY(ClassName)	\
	ClassName(const ClassName &);			\
	void operator=(const ClassName &);

}

#endif
