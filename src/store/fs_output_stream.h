// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_FS_OUTPUT_STREAM_H_
#define ACOUSTID_FS_OUTPUT_STREAM_H_

#include <QSharedPointer>
#include <QString>
#include "fs_file.h"
#include "buffered_output_stream.h"

namespace Acoustid {

class FSOutputStream : public BufferedOutputStream
{
public:
	explicit FSOutputStream(const FSFileSharedPtr &file);
	~FSOutputStream();

	int fileDescriptor() const;

	static FSOutputStream *open(const QString &fileName);

protected:
	size_t write(const uint8_t *data, size_t offset, size_t length);

private:
	FSFileSharedPtr m_file;
};

class NamedFSOutputStream : public FSOutputStream
{
public:
	NamedFSOutputStream(const QString &name, const FSFileSharedPtr &file);

	QString fileName() const;

	static NamedFSOutputStream *openTemporary();

private:
	QString m_fileName;
};

}

#endif

