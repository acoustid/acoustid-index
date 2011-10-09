// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "common.h"
#include "checksum_input_stream.h"

using namespace Acoustid;

ChecksumInputStream::ChecksumInputStream(InputStream *input)
	: m_input(input), m_crc(0)
{
}

ChecksumInputStream::~ChecksumInputStream()
{
}

uint32_t ChecksumInputStream::checksum()
{
	return m_crc;
}

uint8_t ChecksumInputStream::readByte()
{
	uint8_t b = m_input->readByte();
	m_crc = crc_update(m_crc, &b, 1);
	//qDebug() << "reading byte" << b << m_crc;
	return b;
}

size_t ChecksumInputStream::position()
{
	return m_input->position();
}

void ChecksumInputStream::seek(size_t position)
{
	throw IOException("not seekable");
}

