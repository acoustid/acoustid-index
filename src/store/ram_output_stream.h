// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_RAM_OUTPUT_STREAM_H_
#define ACOUSTID_RAM_OUTPUT_STREAM_H_

#include <QBuffer>

#include "output_stream.h"

namespace Acoustid {

class RAMOutputStream : public OutputStream {
 public:
    explicit RAMOutputStream(QByteArray *data);
    ~RAMOutputStream();

    void writeByte(uint8_t b);

    size_t position();
    void seek(size_t position);

 private:
    QBuffer m_buffer;
};

}  // namespace Acoustid

#endif
