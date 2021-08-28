// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>

#include <QSqlQuery>

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
	std::unique_ptr<OutputStream> output(dir.createFile("test.txt"));
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
	std::unique_ptr<OutputStream> output(dir.createFile("test.txt"));
	output->writeByte('a');
	output->writeByte('b');
	output->writeByte('c');
	output->writeByte(0);
	output.reset();
	std::unique_ptr<InputStream> input(dir.openFile("test.txt"));
	ASSERT_EQ('a', input->readByte());
	ASSERT_EQ('b', input->readByte());
	ASSERT_EQ('c', input->readByte());
	ASSERT_EQ(0, input->readByte());
}

TEST(RAMDirectoryTest, OpenNonExistantFile)
{
	RAMDirectory dir;
	ASSERT_THROW(dir.openFile("test.txt"), IOException);
}

TEST(RAMDirectoryTest, DeleteFile)
{
	RAMDirectory dir;
	std::unique_ptr<OutputStream> output(dir.createFile("test.txt"));
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
	std::unique_ptr<OutputStream> output(dir.createFile("test.txt"));
	QStringList files = dir.listFiles();
	ASSERT_EQ(1, files.size());
	ASSERT_STREQ("test.txt", qPrintable(files[0]));
	output.reset();
	dir.renameFile("test.txt", "newtest.txt");
	files = dir.listFiles();
	ASSERT_EQ(1, files.size());
	ASSERT_STREQ("newtest.txt", qPrintable(files[0]));
}

TEST(RAMDirectoryTest, OpenDatabase) {
    RAMDirectory dir;
    auto db = dir.openDatabase("foo.db");

    QSqlQuery query(db);
    query.exec("CREATE TABLE foo (a int)");
    query.exec("INSERT INTO foo (a) VALUES (1)");
    query.exec("SELECT * FROM foo");

    ASSERT_TRUE(query.first());
    ASSERT_EQ(query.value(0).toInt(), 1);
}
