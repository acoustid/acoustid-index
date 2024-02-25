// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "ram_directory.h"

#include "memory_input_stream.h"
#include "ram_output_stream.h"

#include <sqlite3.h>

#include <QRandomGenerator>

using namespace Acoustid;

RAMDirectory::RAMDirectory() : m_data(QSharedPointer<RAMDirectoryData>::create()) {
    m_dbPrefix = QString("%1_").arg(QRandomGenerator::global()->generate());
}

RAMDirectory::RAMDirectory(const QSharedPointer<RAMDirectoryData> &data) : m_data(data) {}

RAMDirectory::~RAMDirectory() {}

void RAMDirectory::close() {}

QString RAMDirectory::path() const {
    return QStringLiteral(":memory:");
}

QStringList RAMDirectory::listFiles() { return m_data->files.keys(); }

bool RAMDirectory::fileExists(const QString &name) { return m_data->files.contains(name); }

void RAMDirectory::deleteFile(const QString &name) {
    if (!m_data->files.contains(name)) {
        return;
    }
    m_data->files.take(name);
}

void RAMDirectory::renameFile(const QString &oldName, const QString &newName) {
    m_data->files.insert(newName, m_data->files.take(oldName));
}

InputStream *RAMDirectory::openFile(const QString &name) {
    auto data = m_data->files.value(name);
    if (!data) {
        throw IOException("file does not exist");
    }
    return new MemoryInputStream(reinterpret_cast<const uint8_t *>(data->constData()), data->size());
}

OutputStream *RAMDirectory::createFile(const QString &name) {
    auto data = QSharedPointer<QByteArray>::create();
    m_data->files.insert(name, data);
    return new RAMOutputStream(data.get());
}

const QByteArray &RAMDirectory::fileData(const QString &name) { return *m_data->files.value(name); }

Directory *RAMDirectory::openDirectory(const QString &name) {
    auto data = m_data->directories.value(name);
    if (!data) {
        data = QSharedPointer<RAMDirectoryData>::create();
        m_data->directories.insert(name, data);
    }
    return new RAMDirectory(data);
}

bool RAMDirectory::exists() { return true; }

void RAMDirectory::ensureExists() {}

void RAMDirectory::deleteDirectory(const QString &name) { m_data->directories.take(name); }

SQLiteDatabase RAMDirectory::openDatabase(const QString &name) {
    auto fileName = QString("file:%1?mode=memory&cache=shared").arg(m_dbPrefix + name);
    return SQLiteDatabase(fileName);
}

QStringList RAMDirectory::listDirectories() {
    return m_data->directories.keys();
}
