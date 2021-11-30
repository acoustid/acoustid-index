// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "segment_merger.h"

#include "index_utils.h"

using namespace Acoustid;

SegmentMerger::SegmentMerger(SegmentDataWriter *writer) : m_writer(writer) {}

SegmentMerger::~SegmentMerger() { qDeleteAll(m_readers); }

size_t SegmentMerger::merge() {
    QList<SegmentEnum *> readers(m_readers);
    QMutableListIterator<SegmentEnum *> iter(readers);
    while (iter.hasNext()) {
        SegmentEnum *reader = iter.next();
        if (!reader->next()) {
            iter.remove();
        }
    }
    uint64_t lastMinItem = UINT64_MAX;
    while (!readers.isEmpty()) {
        size_t minItemIndex = 0;
        uint64_t minItem = UINT64_MAX;
        for (size_t i = 0; i < readers.size(); i++) {
            SegmentEnum *reader = readers[i];
            uint64_t item = packItem(reader->key(), reader->value());
            if (item < minItem) {
                minItem = item;
                minItemIndex = i;
            }
        }
        if (!readers[minItemIndex]->next()) {
            readers.removeAt(minItemIndex);
        }
        if (minItem != lastMinItem) {
            uint32_t key = unpackItemKey(minItem);
            uint32_t value = unpackItemValue(minItem);
            m_writer->addItem(key, value);
            lastMinItem = minItem;
        }
    }
    m_writer->close();
    return m_writer->blockCount();
}
