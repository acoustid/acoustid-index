// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_BUFFERED_OUTPUT_STREAM_H_
#define ACOUSTID_BUFFERED_OUTPUT_STREAM_H_

#include "common.h"
#include "output_stream.h"

namespace Acoustid {

class BufferedOutputStream : public OutputStream {
 public:
    BufferedOutputStream(size_t bufferSize = 1024 * 8);
    ~BufferedOutputStream();

    size_t bufferSize();
    void setBufferSize(size_t size);

    void writeByte(uint8_t);
    void writeBytes(const uint8_t *data, size_t length);

    size_t position();
    void seek(size_t position);
    void flush();

 protected:
    virtual size_t write(const uint8_t *data, size_t offset, size_t length) = 0;
    void flushBuffer();
    void refill();

 private:
    std::unique_ptr<uint8_t[]> m_buffer;
    size_t m_bufferSize;
    size_t m_start;
    size_t m_position;
    size_t m_length;
};

}  // namespace Acoustid

#endif
