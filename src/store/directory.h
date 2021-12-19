// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_DIRECTORY_H_
#define ACOUSTID_DIRECTORY_H_

#include <QString>
#include <QStringList>

#include "common.h"

struct sqlite3;

namespace Acoustid {

class InputStream;
class OutputStream;

class Directory {
 public:
    virtual ~Directory();

    virtual void close() = 0;

    virtual QString path() const = 0;

    virtual OutputStream *createFile(const QString &name) = 0;
    virtual void deleteFile(const QString &name) = 0;
    virtual InputStream *openFile(const QString &name) = 0;
    virtual void renameFile(const QString &oldName, const QString &newName) = 0;
    virtual QStringList listFiles() = 0;
    virtual bool fileExists(const QString &name);

    virtual Directory *openDirectory(const QString &name) = 0;

    virtual bool exists() = 0;
    virtual void ensureExists() = 0;

    virtual void deleteDirectory(const QString &name) = 0;

    virtual sqlite3 *openDatabase(const QString &name) = 0;

    /***
     * Ensure that any writes to these files are moved to
     * stable storage. This is used to properly commit
     * changes to the index, to prevent a machine/OS
     * crash from corrupting the index.
     */
    virtual void sync(const QStringList &names);
};

typedef QWeakPointer<Directory> DirectoryWeakPtr;
typedef QSharedPointer<Directory> DirectorySharedPtr;

}  // namespace Acoustid

#endif
