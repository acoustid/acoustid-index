// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "memory_input_stream.h"
#include "ram_output_stream.h"
#include "ram_directory.h"

using namespace Acoustid;

RAMDirectory::RAMDirectory()
{
}

RAMDirectory::~RAMDirectory()
{
	qDeleteAll(m_data.values());
}

void RAMDirectory::close()
{
}

QStringList RAMDirectory::listFiles()
{
	return m_data.keys();
}

bool RAMDirectory::fileExists(const QString &name)
{
	return m_data.contains(name);
}

void RAMDirectory::deleteFile(const QString &name)
{
	if (!m_data.contains(name)) {
		return;
	}
	delete m_data.take(name);
}

void RAMDirectory::renameFile(const QString &oldName, const QString &newName)
{
	m_data.insert(newName, m_data.take(oldName));
}

InputStream *RAMDirectory::openFile(const QString &name)
{
	QByteArray *data = m_data.value(name);
	if (!data) {
		throw IOException("file does not exist");
	}
	return new MemoryInputStream(reinterpret_cast<const uint8_t *>(data->constData()), data->size());
}

OutputStream *RAMDirectory::createFile(const QString &name)
{
	QByteArray *data = new QByteArray();
	m_data.insert(name, data);
	return new RAMOutputStream(data);
}

const QByteArray &RAMDirectory::fileData(const QString &name)
{
	return *m_data.value(name);
}

QSqlDatabase RAMDirectory::openDatabase(const QString &name)
{
    if (QSqlDatabase::contains(name)) {
        return QSqlDatabase::database(name);
    }
    auto db = QSqlDatabase::addDatabase("QSQLITE", name);
    db.setDatabaseName(":memory:");
    if (!db.open()) {
		throw IOException(QString("Couldn't open the DB file '%1' (%2)").arg(name).arg(db.lastError().text()));
    }
    return db;
}
