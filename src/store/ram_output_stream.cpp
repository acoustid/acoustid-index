// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "ram_output_stream.h"

#include "common.h"

using namespace Acoustid;

RAMOutputStream::RAMOutputStream(QByteArray *data) : m_buffer(data) { m_buffer.open(QBuffer::WriteOnly); }

RAMOutputStream::~RAMOutputStream() {}

void RAMOutputStream::writeByte(uint8_t b) { m_buffer.write(reinterpret_cast<const char *>(&b), 1); }

size_t RAMOutputStream::position() { return m_buffer.pos(); }

void RAMOutputStream::seek(size_t position) { m_buffer.seek(position); }
