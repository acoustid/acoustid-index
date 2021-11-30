// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "input_stream.h"

using namespace Acoustid;

InputStream::~InputStream() {}

QString InputStream::readString() {
    size_t size = readVInt32();
    std::unique_ptr<uint8_t[]> data(new uint8_t[size]);
    for (size_t i = 0; i < size; i++) {
        data[i] = readByte();
    }
    return QString::fromUtf8(reinterpret_cast<const char *>(data.get()), size);
}
