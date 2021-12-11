// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "fs_directory.h"

#include <errno.h>

#include <QByteArray>
#include <QDir>
#include <QFile>
#include <QMutexLocker>
#include <QString>

#include "common.h"
#include "fs_input_stream.h"
#include "fs_output_stream.h"
#include "mmap_input_stream.h"

using namespace Acoustid;

FSDirectory::FSDirectory(const QString &path, bool mmap) : m_path(path), m_mmap(mmap) {}

FSDirectory::~FSDirectory() { close(); }

void FSDirectory::close() {
    QMutexLocker locker(&m_mutex);
    if (m_autoDelete) {
        QDir dir(m_path);
        dir.removeRecursively();
        m_autoDelete = false;
    }
}

OutputStream *FSDirectory::createFile(const QString &name) {
    QMutexLocker locker(&m_mutex);
    QString path = filePath(name);
    return FSOutputStream::open(path);
}

InputStream *FSDirectory::openFile(const QString &name) {
    QMutexLocker locker(&m_mutex);
    QString path = filePath(name);
    FSFileSharedPtr file = m_openInputFiles.value(path);
    if (m_mmap) {
        if (file.isNull()) {
            MMapInputStream *input = MMapInputStream::open(path);
            m_openInputFiles.insert(path, input->file());
            return input;
        }
        return new MMapInputStream(file);
    }
    if (file.isNull()) {
        FSInputStream *input = FSInputStream::open(path);
        m_openInputFiles.insert(path, input->file());
        return input;
    }
    return new FSInputStream(file);
}

void FSDirectory::deleteFile(const QString &name) {
    QMutexLocker locker(&m_mutex);
    QString path = filePath(name);
    m_openInputFiles.remove(path);
    QFile::remove(path);
}

void FSDirectory::renameFile(const QString &oldName, const QString &newName) {
    QMutexLocker locker(&m_mutex);
    QFile::rename(filePath(oldName), filePath(newName));
}

QStringList FSDirectory::listFiles() {
    QMutexLocker locker(&m_mutex);
    QDir dir(m_path);
    return dir.entryList(QStringList(), QDir::Files);
}

bool FSDirectory::fileExists(const QString &name) {
    QMutexLocker locker(&m_mutex);
    return QFile::exists(filePath(name));
}

void FSDirectory::sync(const QStringList &names) {
    for (size_t i = 0; i < names.size(); i++) {
        fsync(names.at(i));
    }
}

void FSDirectory::fsync(const QString &name) {
    QString fileName = filePath(name);
    std::unique_ptr<FSInputStream> input(FSInputStream::open(fileName));
    int ret = ::fsync(input->fileDescriptor());
    if (ret == -1) {
        throw IOException(QString("Couldn't synchronize file '%1' (errno %2)").arg(fileName).arg(errno));
    }
}

Directory *FSDirectory::openDirectory(const QString &name) {
    QMutexLocker locker(&m_mutex);
    QString path = filePath(name);
    return new FSDirectory(path, m_mmap);
}

FSDirectory *FSDirectory::openTemporary(bool autoDelete) {
    QByteArray path("/tmp/acoustidXXXXXX");
    auto tmpPath = ::mkdtemp(path.data());
    if (tmpPath == NULL) {
        throw IOException("couldn't create a temporary directory");
    }
    auto dir = new FSDirectory(path);
    dir->setAutoDelete(autoDelete);
    return dir;
}

bool FSDirectory::exists() {
    QMutexLocker locker(&m_mutex);
    QDir dir(m_path);
    return dir.exists();
}

void FSDirectory::ensureExists() {
    QMutexLocker locker(&m_mutex);
    QDir dir(m_path);
    if (!dir.exists()) {
        if (!dir.mkpath(m_path)) {
            throw IOException(QString("Couldn't create directory '%1'").arg(m_path));
        }
    }
}

void FSDirectory::deleteDirectory(const QString &name) {
    QMutexLocker locker(&m_mutex);
    QDir dir(filePath(name));
    dir.removeRecursively();
}

QSqlDatabase FSDirectory::openDatabase(const QString &name) {
    if (QSqlDatabase::contains(name)) {
        return QSqlDatabase::database(name);
    }
    auto db = QSqlDatabase::addDatabase("QSQLITE", name);
    auto fileName = filePath(name);
    db.setDatabaseName(fileName);
    if (!db.open()) {
        throw IOException(QString("Couldn't open the DB file '%1' (%2)").arg(fileName).arg(db.lastError().text()));
    }
    return db;
}
