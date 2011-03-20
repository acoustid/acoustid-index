// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_STORE_FS_DIRECTORY_H_
#define ACOUSTID_STORE_FS_DIRECTORY_H_

#include <QString>
#include <QHash>
#include <QMutex>
#include "fs_file.h"
#include "directory.h"

namespace Acoustid {

class InputStream;
class OutputStream;

class FSDirectory : public Directory
{
public:
	FSDirectory(const QString &path);
	virtual ~FSDirectory();

	virtual void close();

	virtual OutputStream *createFile(const QString &name);
	virtual void deleteFile(const QString &name);
	virtual InputStream *openFile(const QString &name);
	virtual void renameFile(const QString &oldName, const QString &newName);
	QStringList listFiles();
	bool fileExists(const QString &name);

private:

	QString filePath(const QString &name)
	{
		return m_path + "/" + name;
	}

	QMutex m_mutex;
	QHash<QString, FSFileWeakPtr> m_openInputFiles;
	QString m_path;
};

}

#endif
