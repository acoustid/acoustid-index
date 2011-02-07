// Acoustid Index -- Inverted index for audio fingerprints
// Copyright (C) 2011  Lukas Lalinsky
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "input_stream.h"
#include "output_stream.h"
#include "ram_directory.h"

using namespace Acoustid;

TEST(RAMDirectoryTest, EmptyListFiles)
{
	RAMDirectory dir;
	QStringList files = dir.listFiles();
	ASSERT_TRUE(files.isEmpty());
}

TEST(RAMDirectoryTest, CreateFile)
{
	RAMDirectory dir;
	ScopedPtr<OutputStream> output(dir.createFile("test.txt"));
	QStringList files = dir.listFiles();
	ASSERT_EQ(1, files.size());
	ASSERT_STREQ("test.txt", qPrintable(files[0]));
	output->writeByte('a');
	output->writeByte('b');
	output->writeByte('c');
	output->writeByte(0);
	output.reset();
	ASSERT_STREQ("abc", dir.fileData("test.txt"));
}

TEST(RAMDirectoryTest, OpenFile)
{
	RAMDirectory dir;
	ScopedPtr<OutputStream> output(dir.createFile("test.txt"));
	output->writeByte('a');
	output->writeByte('b');
	output->writeByte('c');
	output->writeByte(0);
	output.reset();
	ScopedPtr<InputStream> input(dir.openFile("test.txt"));
	ASSERT_EQ('a', input->readByte());
	ASSERT_EQ('b', input->readByte());
	ASSERT_EQ('c', input->readByte());
	ASSERT_EQ(0, input->readByte());
}

TEST(RAMDirectoryTest, DeleteFile)
{
	RAMDirectory dir;
	ScopedPtr<OutputStream> output(dir.createFile("test.txt"));
	QStringList files = dir.listFiles();
	ASSERT_EQ(1, files.size());
	ASSERT_STREQ("test.txt", qPrintable(files[0]));
	output.reset();
	dir.deleteFile("test.txt");
	files = dir.listFiles();
	ASSERT_TRUE(files.isEmpty());
}

TEST(RAMDirectoryTest, RenameFile)
{
	RAMDirectory dir;
	ScopedPtr<OutputStream> output(dir.createFile("test.txt"));
	QStringList files = dir.listFiles();
	ASSERT_EQ(1, files.size());
	ASSERT_STREQ("test.txt", qPrintable(files[0]));
	output.reset();
	dir.renameFile("test.txt", "newtest.txt");
	files = dir.listFiles();
	ASSERT_EQ(1, files.size());
	ASSERT_STREQ("newtest.txt", qPrintable(files[0]));
}

