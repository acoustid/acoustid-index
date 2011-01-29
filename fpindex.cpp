#include <QFile>
#include <QTextStream>
#include <QDebug>
#include <stdint.h>
#include <stdio.h>

size_t encodeVInt32(uint32_t value, uint8_t *data, size_t size)
{
	if (value < 128) {
		if (size >= 1) {
			data[0] = value;
			return 1;
		}
		return 0;
	}
	if (value < 128 * 128) {
		if (size >= 2) {
			data[0] = 128 | (value >> 7);
			data[1] = (value & 127);
			return 2;
		}
		return 0;
	}
	if (value < 128 * 128 * 128) {
		if (size >= 3) {
			data[0] = 128 | (value >> 14);
			data[1] = 128 | ((value >> 7) & 127);
			data[2] = (value & 127);
			return 3;
		}
		return 0;
	}
	if (value < 128 * 128 * 128 * 128) {
		if (size >= 4) {
			data[0] = 128 | (value >> 21);
			data[1] = 128 | ((value >> 14) & 127);
			data[2] = 128 | ((value >> 7) & 127);
			data[3] = (value & 127);
			return 4;
		}
		return 0;
	}
	if (size >= 5) {
		data[0] = 128 | (value >> 28);
		data[1] = 128 | ((value >> 21) & 127);
		data[2] = 128 | ((value >> 14) & 127);
		data[3] = 128 | ((value >> 7) & 127);
		data[4] = (value & 127);
		return 5;
	}
	return 0;
}

const int BLOCK_HEADER_SIZE = 2;
const int BLOCK_SIZE = 128;
const int BLOCK_DATA_SIZE = BLOCK_SIZE - BLOCK_HEADER_SIZE;

class BlockWriter
{
public:
	BlockWriter(QFile *out);

	void reset();
	void write(unsigned int key, unsigned int value);
	void flush();

	QList<unsigned int> keys() const
	{
		return m_keys;
	}

protected:
	void newBlock(unsigned int key);

private:
	size_t m_blockKeyCount;
	uint8_t m_data[BLOCK_SIZE];
	uint8_t *m_ptr;
	size_t m_size;
	unsigned int m_last_key;
	unsigned int m_last_value;
	QList<unsigned int> m_keys;
	QFile *m_out;
};

BlockWriter::BlockWriter(QFile *out)
{
	m_ptr = 0;
	m_out = out;
}

void BlockWriter::reset()
{
	m_ptr = 0;
	m_keys.clear();
}

void BlockWriter::newBlock(unsigned int key)
{
	memset(m_data, 0, sizeof(m_data));
	m_ptr = m_data + BLOCK_HEADER_SIZE;
	m_size = BLOCK_DATA_SIZE;
	m_last_key = key;
	m_last_value = 0;
	m_keys.append(key);
	m_blockKeyCount = 0;
}

void BlockWriter::write(unsigned int key, unsigned int value)
{
	if (m_ptr == 0) {
		newBlock(key);
	}
	if (m_last_key != key) {
		m_last_value = 0;
	}

	unsigned int key_delta = key - m_last_key;
	unsigned int value_delta = value - m_last_value;

	bool needsNewBlock = false;
	size_t key_delta_size, value_delta_size;

writeBlockLabel:

	if (m_blockKeyCount == 0) {
		value_delta_size = encodeVInt32(value_delta, m_ptr, m_size);
		if (value_delta_size) {
			m_size -= value_delta_size;
			m_ptr += value_delta_size;
			m_blockKeyCount += 1;
		}
		else {
			needsNewBlock = true;
		}
	}
	else {
		key_delta_size = encodeVInt32(key_delta, m_ptr, m_size);
		if (key_delta_size) {
			m_size -= key_delta_size;
			m_ptr += key_delta_size;
			value_delta_size = encodeVInt32(value_delta, m_ptr, m_size);
			if (value_delta_size) {
				m_size -= value_delta_size;
				m_ptr += value_delta_size;
				m_blockKeyCount += 1;
			}
			else {
				m_size += key_delta_size;
				m_ptr -= key_delta_size;
				needsNewBlock = true;
			}
		}
		else {
			needsNewBlock = true;
		}
	}

	if (needsNewBlock) {
		m_data[0] = (m_blockKeyCount >> 8) & 0xff;
		m_data[1] = (m_blockKeyCount     ) & 0xff;
		m_out->write((char *) m_data, BLOCK_SIZE);
		newBlock(key);
		needsNewBlock = false;
		goto writeBlockLabel;
		key_delta = 0;
		value_delta = value;
	}

	m_last_key = key;
	m_last_value = value;
}

void BlockWriter::flush()
{
	if (m_size != BLOCK_DATA_SIZE) {
		m_out->write((char *) m_data, BLOCK_SIZE);
	}
}

int main(int argc, char **argv)
{
	uint8_t buffer[BLOCK_SIZE];
	uint8_t *pos = buffer + BLOCK_HEADER_SIZE;
	size_t size = BLOCK_DATA_SIZE;
	ulong nitems = 0, ntotalitems = 0, nblocks = 0;
	uint32_t first_key = 0;
	uint32_t last_key, last_value;

	QTextStream in(stdin);
	QFile out("segment0.fid");
	out.open(QIODevice::WriteOnly);

	QList< QPair<int, int> > positions;

	memset(buffer, 0, sizeof(buffer));

	BlockWriter writer(&out);
	unsigned long blockCount = 0;

	qDebug() << "Writing leaf data";
	writer.reset();
	unsigned long valueCount = 0;
	while (!in.atEnd()) {
		unsigned int key, value;
		in >> key >> value;
		if (in.status() != QTextStream::Ok)
			break;
		valueCount++;
		writer.write(key, value);
	}
	writer.flush();

	unsigned long level = 0;
	int indexInterval = 64;
	int skip = 1;
	uint8_t data[10];
	QList<unsigned int> keys = writer.keys();
	qDebug() << "Total" << valueCount << "value in" << keys.size() << "blocks";
	QFile out2("segment0.fii");
	out2.open(QIODevice::WriteOnly);
	size_t encodedSize = encodeVInt32(indexInterval, data, 10);
	out2.write((char *)data, encodedSize);
	while (keys.size() > skip) {
		size_t levelKeyCount = ((keys.size() + skip - 1) / skip);
		encodedSize = encodeVInt32(levelKeyCount, data, 10);
		out2.write((char *)data, encodedSize);
		qDebug() << "Writing" << level++ << "level index with" << levelKeyCount << "entries";
		unsigned long lastKey = 0;
		for (int i = 0; i < keys.size(); i += skip) {
			unsigned long key = keys.at(i);
			encodedSize = encodeVInt32(key - lastKey, data, 10);
			lastKey = key;
			out2.write((char *)data, encodedSize);
		}
		skip *= indexInterval;
	}

	out.close();
	out2.close();

	return 0;
}

