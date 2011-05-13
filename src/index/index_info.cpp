#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "index_info.h"

using namespace Acoustid;

int IndexInfo::segmentsRevision(const QString &fileName)
{
	return fileName.mid(9).toInt();
}

QString IndexInfo::segmentsFileName(int revision)
{
	return QString("segments_%1").arg(revision);
}

int IndexInfo::findCurrentRevision(Directory *dir)
{
	QStringList files = dir->listFiles();
	int currentRev = -1;
	for (size_t i = 0; i < files.size(); i++) {
		QString file = files.at(i);
		if (file.startsWith("segments_")) {
			int rev = segmentsRevision(file);
			if (rev > currentRev) {
				currentRev = rev;
			}
		}
	}
/*	if (currentRev == -1) {
		throw IOError("No segments file found");
	}*/
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

void IndexInfo::read(InputStream *input)
{
	clear();
	setLastSegmentId(input->readVInt32());
	size_t segmentCount = input->readVInt32();
	for (size_t i = 0; i < segmentCount; i++) {
		uint32_t id = input->readVInt32();
		uint32_t blockCount = input->readVInt32();
		uint32_t lastKey = input->readVInt32();
		add(SegmentInfo(id, blockCount, lastKey));
	}
	delete input;
}

void IndexInfo::write(OutputStream *output)
{
	output->writeVInt32(lastSegmentId());
	output->writeVInt32(segmentCount());
	for (size_t i = 0; i < segmentCount(); i++) {
		output->writeVInt32(info(i).id());
		output->writeVInt32(info(i).blockCount());
		output->writeVInt32(info(i).lastKey());
	}
}

