// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_FS_INPUT_STREAM_H_
#define ACOUSTID_FS_INPUT_STREAM_H_

#include "fs_file.h"
#include "buffered_input_stream.h"

namespace Acoustid {

class FSInputStream : public BufferedInputStream
{
public:
	explicit FSInputStream(const FSFileSharedPtr &file);
	~FSInputStream();

	int fileDescriptor() const;
	const FSFileSharedPtr &file() const;

	static FSInputStream *open(const QString &fileName);

protected:
	size_t read(uint8_t *data, size_t offset, size_t length);

private:
	FSFileSharedPtr m_file;
};

}

#endif

