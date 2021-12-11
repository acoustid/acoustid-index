// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_STORE_FS_DIRECTORY_H_
#define ACOUSTID_STORE_FS_DIRECTORY_H_

#include <QHash>
#include <QMutex>
#include <QString>

#include "directory.h"
#include "fs_file.h"

namespace Acoustid {

class InputStream;
class OutputStream;

class FSDirectory : public Directory {
 public:
    FSDirectory(const QString &path, bool mmap = false);
    virtual ~FSDirectory();

    virtual void close();

    virtual OutputStream *createFile(const QString &name);
    virtual void deleteFile(const QString &name);
    virtual InputStream *openFile(const QString &name);
    virtual void renameFile(const QString &oldName, const QString &newName);
    QStringList listFiles();
    bool fileExists(const QString &name);
    virtual void sync(const QStringList &names);

    virtual QSqlDatabase openDatabase(const QString &name) override;

    virtual bool exists() override;
    virtual void ensureExists() override;

    virtual Directory *openDirectory(const QString &name);
    virtual void deleteDirectory(const QString &name) override;

    void setAutoDelete(bool autoDelete) { m_autoDelete = autoDelete; }
    bool autoDelete() const { return m_autoDelete; }

    static FSDirectory *openTemporary(bool autoDelete = false);

 private:
    void fsync(const QString &name);

    QString filePath(const QString &name) { return m_path + "/" + name; }

    bool m_mmap;
    QMutex m_mutex;
    QHash<QString, FSFileSharedPtr> m_openInputFiles;
    QString m_path;
    bool m_autoDelete{false};
};

}  // namespace Acoustid

#endif
