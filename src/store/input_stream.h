// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_STORE_INPUT_STREAM_H_
#define ACOUSTID_STORE_INPUT_STREAM_H_

#include "common.h"

namespace Acoustid {

class InputStream {
 public:
    virtual ~InputStream();

    virtual uint8_t readByte() = 0;

    virtual uint16_t readInt16() { return (readByte() << 8) | readByte(); }

    virtual uint32_t readInt32() { return (readByte() << 24) | (readByte() << 16) | (readByte() << 8) | readByte(); }

    virtual uint32_t readVInt32() {
        uint8_t b = readByte();
        uint32_t i = b & 0x7f;
        int shift = 7;
        while (b & 0x80) {
            b = readByte();
            i |= (b & 0x7f) << shift;
            shift += 7;
        }
        return i;
    }

    virtual QString readString();

    virtual size_t position() = 0;
    virtual void seek(size_t position) = 0;
};

}  // namespace Acoustid

#endif
