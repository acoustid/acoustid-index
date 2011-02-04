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

#include <QString>
#include <QByteArray>
#include <QFile>
#include "common.h"
#include "fs_input_stream.h"
#include "fs_output_stream.h"
#include "fs_directory.h"

FSDirectory::FSDirectory(const QString &path)
	: m_path(path)
{
}

FSDirectory::~FSDirectory()
{
}

void FSDirectory::close()
{
}

OutputStream *FSDirectory::createFile(const QString &name)
{
	return FSOutputStream::open(filePath(name));
}

InputStream *FSDirectory::openFile(const QString &name)
{
	return FSInputStream::open(filePath(name));
}

void FSDirectory::deleteFile(const QString &name)
{
	QFile::remove(filePath(name));
}

void FSDirectory::renameFile(const QString &oldName, const QString &newName)
{
	QFile::rename(filePath(oldName), filePath(newName));
}


