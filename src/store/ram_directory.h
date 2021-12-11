// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_STORE_RAM_DIRECTORY_H_
#define ACOUSTID_STORE_RAM_DIRECTORY_H_

#include <QHash>
#include <QString>

#include "common.h"
#include "directory.h"

namespace Acoustid {

class InputStream;
class OutputStream;

struct RAMDirectoryData {
    QHash<QString, QSharedPointer<QByteArray>> files;
    QHash<QString, QSharedPointer<RAMDirectoryData>> directories;
};

class RAMDirectory : public Directory {
 public:
    RAMDirectory();
    virtual ~RAMDirectory();

    virtual void close();

    const QByteArray &fileData(const QString &name);

    virtual OutputStream *createFile(const QString &name);
    virtual void deleteFile(const QString &name);
    virtual InputStream *openFile(const QString &name);
    virtual void renameFile(const QString &oldName, const QString &newName);
    QStringList listFiles();
    bool fileExists(const QString &name);

    virtual QSqlDatabase openDatabase(const QString &name) override;

    virtual bool exists() override;
    virtual void ensureExists() override;

    virtual Directory *openDirectory(const QString &name) override;

    virtual void deleteDirectory(const QString &name) override;

 private:
    RAMDirectory(const QSharedPointer<RAMDirectoryData> &data);

    QSharedPointer<RAMDirectoryData> m_data;
};

}  // namespace Acoustid

#endif
