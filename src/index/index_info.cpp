#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "index_info.h"

using namespace Acoustid;

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

void IndexInfo::clear()
{
	m_infos.clear();
}

void IndexInfo::add(const SegmentInfo &info)
{
	m_infos.append(info);
}

bool IndexInfo::load(Directory* dir)
{
	m_revision = IndexInfo::findCurrentRevision(dir);
	if (m_revision != -1) {
		load(dir->openFile(indexInfoFileName(m_revision)));
		return true;
	}
	return false;
}

void IndexInfo::load(InputStream* input)
{
	ScopedPtr<InputStream> guard(input);
	clear();
	setLastSegmentId(input->readVInt32());
	size_t segmentCount = input->readVInt32();
	for (size_t i = 0; i < segmentCount; i++) {
		uint32_t id = input->readVInt32();
		uint32_t blockCount = input->readVInt32();
		uint32_t lastKey = input->readVInt32();
		add(SegmentInfo(id, blockCount, lastKey));
	}
}

void IndexInfo::save(Directory* dir)
{
	m_revision++;
	QString fileName = indexInfoFileName(m_revision);
	QString tempFileName = fileName + ".tmp";
	save(dir->createFile(tempFileName));
	dir->renameFile(tempFileName, fileName);
}

void IndexInfo::save(OutputStream *output)
{
	ScopedPtr<OutputStream> guard(output);
	output->writeVInt32(lastSegmentId());
	output->writeVInt32(segmentCount());
	for (size_t i = 0; i < segmentCount(); i++) {
		output->writeVInt32(info(i).id());
		output->writeVInt32(info(i).blockCount());
		output->writeVInt32(info(i).lastKey());
	}
}

