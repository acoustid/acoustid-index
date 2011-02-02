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

#ifndef ACOUSTID_UTIL_SEARCH_UTILS_H_
#define ACOUSTID_UTIL_SEARCH_UTILS_H_

#include "common.h"
#include <algorithm>

/**
 * Find the position of the first element within the range [lo,hi) of the
 * sorted array that is smaller than the specified value. Returns -1 if
 * no such element exists.
 */
template<typename T>
inline ssize_t searchFirstSmaller(T *data, size_t lo, size_t hi, T value)
{
	ssize_t index = std::lower_bound(data + lo, data + hi, value) - data;
	return index - 1;
}

/**
 * Find the position of the last element within the range [lo,hi) of the
 * sorted array that is greater than the specified value.
 */
template<typename T>
inline ssize_t scanFirstGreater(T *data, size_t lo, size_t hi, T value)
{
	while (lo < hi && data[lo] <= value) {
		++lo;
	}
	return lo;
}

#endif
