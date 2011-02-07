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

#ifndef ACOUSTID_DIRECTORY_H_
#define ACOUSTID_DIRECTORY_H_

#include <QString>
#include <QStringList>

namespace Acoustid {

class InputStream;
class OutputStream;

class Directory {

public:
	virtual ~Directory();

	virtual void close() = 0;

	virtual OutputStream *createFile(const QString &name) = 0;
	virtual void deleteFile(const QString &name) = 0;
	virtual InputStream *openFile(const QString &name) = 0;
	virtual void renameFile(const QString &oldName, const QString &newName) = 0;
	virtual QStringList listFiles() = 0;

};

}

#endif
