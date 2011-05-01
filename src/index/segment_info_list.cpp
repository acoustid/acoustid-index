#include "store/directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "segment_info_list.h"

using namespace Acoustid;

int SegmentInfoList::segmentsRevision(const QString &fileName)
{
	return fileName.mid(9).toInt();
}

QString SegmentInfoList::segmentsFileName(int revision)
{
	return QString("segments_%1").arg(revision);
}

int SegmentInfoList::findCurrentRevision(Directory *dir)
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

void SegmentInfoList::clear()
{
	m_infos.clear();
}

void SegmentInfoList::add(const SegmentInfo &info)
{
	m_infos.append(info);
}

void SegmentInfoList::read(InputStream *input)
{
	clear();
	setNextSegmentNum(input->readVInt32());
	size_t segmentCount = input->readVInt32();
	for (size_t i = 0; i < segmentCount; i++) {
		int id = input->readVInt32();
		size_t numDocs = input->readVInt32();
		add(SegmentInfo(id, numDocs));
	}
}

void SegmentInfoList::write(OutputStream *output)
{
	output->writeVInt32(lastSegmetNum());
	output->writeVInt32(segmentCount());
	for (size_t i = 0; i < segmentCount(); i++) {
		output->writeVInt32(info(i).id());
		output->writeVInt32(info(i).numDocs());
	}
}

