#include <stdio.h>

#include <QDebug>
#include <QFile>
#include <QList>
#include <QTextStream>

#include "index/index_reader.h"
#include "store/fs_directory.h"
#include "util/options.h"

using namespace Acoustid;

int main(int argc, char **argv) {
    OptionParser parser("%prog [options]");
    parser.addOption("directory", 'd').setArgument().setHelp("index directory").setMetaVar("DIR");
    Options *opts = parser.parse(argc, argv);

    QString path = ".";
    if (opts->contains("directory")) {
        path = opts->option("directory");
    }

    FSDirectory dir(path);
    IndexReader reader(&dir);
    try {
        reader.open();
    } catch (IOException &ex) {
        qCritical() << "ERROR:" << ex.what();
        return 1;
    }

    QTextStream out(stdout);
    out << "Revision: " << reader.info().revision() << endl;
    const SegmentInfoList &segments = reader.info().segments();
    out << "Segments: " << segments.size() << endl;
    for (int i = 0; i < segments.size(); i++) {
        const SegmentInfo &segment = segments.at(i);
        out << "Segment " << segment.id() << ": " << segment.blockCount() << endl;
    }

    return 0;
}
