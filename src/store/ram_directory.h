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

#ifndef ACOUSTID_STORE_RAM_DIRECTORY_H_
#define ACOUSTID_STORE_RAM_DIRECTORY_H_

#include <QString>
#include <QHash>
#include "common.h"
#include "directory.h"

namespace Acoustid {

class InputStream;
class OutputStream;

class RAMDirectory : public Directory
{
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

private:
	QHash<QString, QByteArray*> m_data;
};

}

#endif
