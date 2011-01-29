#ifndef ACOUSTID_DIRECTORY_H_
#define ACOUSTID_DIRECTORY_H_

#include <QString>

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

};

#endif
