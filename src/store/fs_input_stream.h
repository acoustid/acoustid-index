#ifndef ACOUSTID_FS_INPUT_STREAM_H_
#define ACOUSTID_FS_INPUT_STREAM_H_

#include "buffered_input_stream.h"

class FSInputStream : public BufferedInputStream
{
public:
	explicit FSInputStream(int fd);
	~FSInputStream();

	int fileDescriptor() const;

	static FSInputStream *open(const QString &fileName);

protected:
	size_t read(uint8_t *data, size_t offset, size_t length);

private:
	int m_fd;	
};

#endif

