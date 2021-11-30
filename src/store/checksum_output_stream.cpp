// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "common.h"
#include "checksum_output_stream.h"

using namespace Acoustid;

ChecksumOutputStream::ChecksumOutputStream(OutputStream *output)
	: m_output(output), m_crc(0)
{
}

ChecksumOutputStream::~ChecksumOutputStream()
{
}

uint32_t ChecksumOutputStream::checksum()
{
	return m_crc;
}

void ChecksumOutputStream::writeByte(uint8_t b)
{
	m_crc = crc_update(m_crc, &b, 1);
	//qDebug() << "writing byte" << b << m_crc;
	m_output->writeByte(b);
}

size_t ChecksumOutputStream::position()
{
	return m_output->position();
}

void ChecksumOutputStream::seek(size_t position)
{
	throw IOException("not seekable");
}

