// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_DIRECTORY_H_
#define ACOUSTID_DIRECTORY_H_

#include <QString>
#include <QStringList>
#include "common.h"

namespace Acoustid {

class InputStream;
class OutputStream;

class Directory {

public:
	virtual ~Directory();

	virtual void close() = 0;

	virtual OutputStream *createFile(const QString &name) = 0;
	virtual void deleteFile(const QString &name) = 0;
	virtual InputStream *openFile(const QString &name) = 0;
	virtual void renameFile(const QString &oldName, const QString &newName) = 0;
	virtual QStringList listFiles() = 0;
	virtual bool fileExists(const QString &name);

	/***
	 * Ensure that any writes to these files are moved to
	 * stable storage. This is used to properly commit
	 * changes to the index, to prevent a machine/OS
	 * crash from corrupting the index.
	 */
	virtual void sync(const QStringList& names);
};

typedef QWeakPointer<Directory> DirectoryWeakPtr;
typedef QSharedPointer<Directory> DirectorySharedPtr;

}

#endif
