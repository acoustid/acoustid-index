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
