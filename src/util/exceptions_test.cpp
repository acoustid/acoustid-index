// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <gtest/gtest.h>
#include "exceptions.h"

using namespace Acoustid;

TEST(ExceptionsTest, TestThrowException)
{
	ASSERT_THROW({ throw IOException("test"); }, Exception);
}

TEST(ExceptionsTest, What)
{
	try {
		throw IOException("test");
	}
	catch (const Exception &ex) {
		ASSERT_STREQ("test", ex.what());
		return;
	}
	ASSERT_TRUE(false);
}
