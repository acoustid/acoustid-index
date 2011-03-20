// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

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
