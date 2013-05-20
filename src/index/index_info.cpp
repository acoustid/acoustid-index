// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_document_reader.h"
#include "segment_index_reader.h"
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

int IndexInfo::findCurrentRevision(Directory* dir, int maxRevision)
{
	const QStringList& fileNames = dir->listFiles();
	int currentRev = -1;
	for (size_t i = 0; i < fileNames.size(); i++) {
		const QString& fileName = fileNames.at(i);
		if (fileName.startsWith("info_")) {
			int rev = indexInfoRevision(fileName);
			if (rev > currentRev && (!maxRevision || rev < maxRevision)) {
				currentRev = rev;
			}
		}
	}
	return currentRev;
}

bool IndexInfo::load(Directory* dir, bool loadIndexes)
{
	int revision = 0;
	while (true) {
		revision = IndexInfo::findCurrentRevision(dir, revision);
		if (revision < 0) {
			break;
		}
		try {
			load(dir->openFile(indexInfoFileName(revision)), loadIndexes, dir);
			d->revision = revision;
			return true;
		}
		catch (IOException& ex) {
			qDebug() << "Corrupt index info" << revision;
			if (revision > 0) {
				continue;
			}
			throw CorruptIndexException(ex.message());
		}
	}
	return false;
}

void IndexInfo::load(InputStream* rawInput, bool loadIndexes, Directory* dir)
{
	ScopedPtr<ChecksumInputStream> input(new ChecksumInputStream(rawInput));
	setLastSegmentId(input->readVInt32());
	clearSegments();
	size_t segmentCount = input->readVInt32();
	for (size_t i = 0; i < segmentCount; i++) {
		uint32_t id = input->readVInt32();
		uint32_t blockCount = input->readVInt32();
		uint32_t fingerprintCount = input->readVInt32();
		uint32_t lastKey = input->readVInt32();
		uint32_t checksum = input->readVInt32();
		SegmentInfo segment(id, blockCount, fingerprintCount, lastKey, checksum);
		if (loadIndexes) {
			segment.setIndex(SegmentIndexReader(dir->openFile(segment.indexFileName()), segment.blockCount()).read());
			segment.setDocumentIndex(SegmentDocumentReader::readIndex(
				dir->openFile(segment.documentIndexFileName()),
				fingerprintCount));
		}
		addSegment(segment);
	}
	size_t attribsCount = input->readVInt32();
	for (size_t i = 0; i < attribsCount; i++) {
		QString name = input->readString();
		QString value = input->readString();
		setAttribute(name, value);
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
		output->writeVInt32(d->segments.at(i).documentCount());
		output->writeVInt32(d->segments.at(i).lastKey());
		output->writeVInt32(d->segments.at(i).checksum());
	}
	{
		QMapIterator<QString, QString> i(d->attribs);
		output->writeVInt32(d->attribs.size());
		while (i.hasNext()) {
			i.next();
			output->writeString(i.key());
			output->writeString(i.value());
		}
	}
	output->flush();
	output->writeInt32(output->checksum());
}

