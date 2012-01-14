// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_MMAP_INPUT_STREAM_H_
#define ACOUSTID_MMAP_INPUT_STREAM_H_

#include "fs_file.h"
#include "memory_input_stream.h"

namespace Acoustid {

class MMapInputStream : public MemoryInputStream
{
public:
	explicit MMapInputStream(const FSFileSharedPtr &file);

	int fileDescriptor() const;
	const FSFileSharedPtr &file() const;

	static MMapInputStream *open(const QString &fileName);

private:
	FSFileSharedPtr m_file;
};

}

#endif

