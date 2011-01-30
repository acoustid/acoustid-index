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

#ifndef ACOUSTID_UTIL_TEST_UTILS_H_
#define ACOUSTID_UTIL_TEST_UTILS_H_

#define ASSERT_INTARRAY_EQ(v1, v2, length) \
	for (size_t i = 0; i < (length); i++) { \
		ASSERT_EQ((long long)(v1)[i], (long long)(v2)[i]) << "Different value at index " << i; \
	} 

#endif

