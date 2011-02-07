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

#include "ram_directory.h"

QStringList RAMDirectory::listFiles()
{
	return m_fileList;
}

void RAMDirectory::deleteFile(const QString &name)
{
	if (!m_data.contains(name)) {
		return;
	}
	m_names.removeAll(name);
	delete m_data.take(name);
}

void RAMDirectory::renameFile(const QString &oldName, const QString &newName)
{
	m_names.removeAll(oldName);
	m_names.append(newName);
	m_data.insert(newName, m_data.take(oldName));
}

InputStream *RAMDirectory::openFile(const QString &name)
{
	QByteArray *data = m_data.value(name);
	if (!data) {
		return NULL;
	}
	return new RAMInputStream(data->constData(), data->size());
}

OutputStream *RAMDirectory::createFile(const QString &name)
{
	QByteArray *data = new QByteArray();
	m_data.insert(name, data);
	return new RAMOutputStream(data);
}

