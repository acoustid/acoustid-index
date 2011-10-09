// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "store/checksum_input_stream.h"
#include "store/checksum_output_stream.h"
#include "index_info.h"

using namespace Acoustid;

QList<QString> IndexInfo::files(bool includeIndexInfo) const
{
	QList<QString> files;
	if (d->revision < 0) {
		return files;
	}
	if (includeIndexInfo) {
		files.append(indexInfoFileName(d->revision));
	}
	for (size_t i = 0; i < d->segments.size(); i++) {
		const SegmentInfo& segment = d->segments.at(i);
		files.append(segment.files());
	}
	return files;
}

int IndexInfo::indexInfoRevision(const QString& fileName)
{
	return fileName.mid(5).toInt();
}

QString IndexInfo::indexInfoFileName(int revision)
{
	return QString("info_%1").arg(revision);
}

int IndexInfo::findCurrentRevision(Directory* dir)
{
	const QStringList& fileNames = dir->listFiles();
	int currentRev = -1;
	for (size_t i = 0; i < fileNames.size(); i++) {
		const QString& fileName = fileNames.at(i);
		if (fileName.startsWith("info_")) {
			int rev = indexInfoRevision(fileName);
			if (rev > currentRev) {
				currentRev = rev;
			}
		}
	}
	return currentRev;
}

bool IndexInfo::load(Directory* dir)
{
	d->revision = IndexInfo::findCurrentRevision(dir);
	if (d->revision != -1) {
		load(dir->openFile(indexInfoFileName(d->revision)));
		return true;
	}
	return false;
}

void IndexInfo::load(InputStream* rawInput)
{
	ScopedPtr<ChecksumInputStream> input(new ChecksumInputStream(rawInput));
	setLastSegmentId(input->readVInt32());
	clearSegments();
	size_t segmentCount = input->readVInt32();
	for (size_t i = 0; i < segmentCount; i++) {
		uint32_t id = input->readVInt32();
		uint32_t blockCount = input->readVInt32();
		uint32_t lastKey = input->readVInt32();
		uint32_t checksum = input->readVInt32();
		addSegment(SegmentInfo(id, blockCount, lastKey, checksum));
	}
	uint32_t expectedChecksum = input->checksum();
	uint32_t checksum = input->readInt32();
	if (checksum != expectedChecksum) {
		throw CorruptIndexException(QString("checksum mismatch %1 != %2").arg(expectedChecksum).arg(checksum));
	}
}

void IndexInfo::save(Directory* dir)
{
	dir->sync(files(false));
	d->revision++;
	QString fileName = indexInfoFileName(d->revision);
	QString tempFileName = fileName + ".tmp";
	save(dir->createFile(tempFileName));
	dir->sync(QStringList() << tempFileName);
	dir->renameFile(tempFileName, fileName);
	dir->sync(QStringList() << fileName);
}

void IndexInfo::save(OutputStream *rawOutput)
{
	ScopedPtr<ChecksumOutputStream> output(new ChecksumOutputStream(rawOutput));
	output->writeVInt32(lastSegmentId());
	output->writeVInt32(segmentCount());
	for (size_t i = 0; i < segmentCount(); i++) {
		output->writeVInt32(d->segments.at(i).id());
		output->writeVInt32(d->segments.at(i).blockCount());
		output->writeVInt32(d->segments.at(i).lastKey());
		output->writeVInt32(d->segments.at(i).checksum());
	}
	output->flush();
	output->writeInt32(output->checksum());
}

