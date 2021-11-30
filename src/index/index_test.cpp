// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "util/test_utils.h"
#include "store/ram_directory.h"
#include "store/input_stream.h"
#include "store/output_stream.h"
#include "index.h"
#include "index_writer.h"

using namespace Acoustid;

TEST(IndexTest, OpenEmpty)
{
	DirectorySharedPtr dir(new RAMDirectory());
	ASSERT_FALSE(dir->fileExists("info_1"));
	ASSERT_THROW({ Index index(dir); }, IOException);
}

TEST(IndexTest, OpenEmptyCreate)
{
	DirectorySharedPtr dir(new RAMDirectory());
	ASSERT_FALSE(dir->fileExists("info_1"));
	Index index(dir, true);
	ASSERT_TRUE(dir->fileExists("info_1"));
}

TEST(IndexTest, DeleteUnusedFiled)
{
	DirectorySharedPtr dir(new RAMDirectory());
	IndexSharedPtr index(new Index(dir, true));

	ASSERT_TRUE(index->directory()->fileExists("info_1"));
	{
		std::unique_ptr<IndexWriter> writer(new IndexWriter(index));
		writer->insertOrUpdateDocument(1, QVector<uint32_t>{ 1, 2, 3 });
		writer->commit();
	}
	ASSERT_TRUE(index->directory()->fileExists("info_2"));
	ASSERT_FALSE(index->directory()->fileExists("info_1"));
}

TEST(IndexTest, Insert)
{
	auto dir = QSharedPointer<RAMDirectory>::create();
	auto index = QSharedPointer<Index>::create(dir, true);

    ASSERT_FALSE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(0, results.size());
    }

    {
        OpBatch batch;
        batch.insertOrUpdateDocument(1, QVector<uint32_t>{ 1, 2, 3 });
        index->applyUpdates(batch);
    }
    ASSERT_TRUE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results.at(0).docId());
    }

	index = QSharedPointer<Index>::create(dir, false);

    ASSERT_TRUE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results.at(0).docId());
    }
}

TEST(IndexTest, InsertAndUpdate)
{
	auto dir = QSharedPointer<RAMDirectory>::create();
	auto index = QSharedPointer<Index>::create(dir, true);

    ASSERT_FALSE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(0, results.size());
    }

    {
        OpBatch batch;
        batch.insertOrUpdateDocument(1, QVector<uint32_t>{ 1, 2, 3 });
        index->applyUpdates(batch);
    }
    ASSERT_TRUE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results.at(0).docId());
    }

    {
        OpBatch batch;
        batch.insertOrUpdateDocument(1, QVector<uint32_t>{ 5, 6, 7 });
        index->applyUpdates(batch);
    }
    ASSERT_TRUE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(0, results.size());
    }
    {
        auto results = index->search(QVector<uint32_t>{ 5, 6, 7 });
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results.at(0).docId());
    }

	index = QSharedPointer<Index>::create(dir, false);

    ASSERT_TRUE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(0, results.size());
    }
    {
        auto results = index->search(QVector<uint32_t>{ 5, 6, 7 });
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results.at(0).docId());
    }
}

TEST(IndexTest, InsertAndDelete)
{
	auto dir = QSharedPointer<RAMDirectory>::create();
	auto index = QSharedPointer<Index>::create(dir, true);

    ASSERT_FALSE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(0, results.size());
    }

    {
        OpBatch batch;
        batch.insertOrUpdateDocument(1, QVector<uint32_t>{ 1, 2, 3 });
        index->applyUpdates(batch);
    }
    ASSERT_TRUE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results.at(0).docId());
    }

    {
        OpBatch batch;
        batch.deleteDocument(1);
        index->applyUpdates(batch);
    }
    ASSERT_FALSE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(0, results.size());
    }

	index = QSharedPointer<Index>::create(dir, false);

    ASSERT_FALSE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(0, results.size());
    }

}

TEST(IndexTest, InsertAndDeleteAndInsert)
{
	auto dir = QSharedPointer<RAMDirectory>::create();
	auto index = QSharedPointer<Index>::create(dir, true);

    ASSERT_FALSE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(0, results.size());
    }

    {
        OpBatch batch;
        batch.insertOrUpdateDocument(1, QVector<uint32_t>{ 1, 2, 3 });
        index->applyUpdates(batch);
    }
    ASSERT_TRUE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results.at(0).docId());
    }

    {
        OpBatch batch;
        batch.deleteDocument(1);
        index->applyUpdates(batch);
    }
    ASSERT_FALSE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(0, results.size());
    }

    {
        OpBatch batch;
        batch.insertOrUpdateDocument(1, QVector<uint32_t>{ 5, 6, 7 });
        index->applyUpdates(batch);
    }
    ASSERT_TRUE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(0, results.size());
    }
    {
        auto results = index->search(QVector<uint32_t>{ 5, 6, 7 });
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results.at(0).docId());
    }

	index = QSharedPointer<Index>::create(dir, false);

    ASSERT_TRUE(index->containsDocument(1));
    {
        auto results = index->search(QVector<uint32_t>{ 1, 2, 3 });
        ASSERT_EQ(0, results.size());
    }
    {
        auto results = index->search(QVector<uint32_t>{ 5, 6, 7 });
        ASSERT_EQ(1, results.size());
        ASSERT_EQ(1, results.at(0).docId());
    }
}
