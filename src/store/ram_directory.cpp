// Acoustid Index -- Inverted index for audio fingerprints
// Copyright (C) 2011  Lukas Lalinsky
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#include "memory_input_stream.h"
#include "ram_output_stream.h"
#include "ram_directory.h"

using namespace Acoustid;

RAMDirectory::RAMDirectory()
{
}

RAMDirectory::~RAMDirectory()
{
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
		return NULL;
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

