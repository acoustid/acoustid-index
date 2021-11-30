// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_UTILS_H_
#define ACOUSTID_INDEX_UTILS_H_

#include "common.h"

namespace Acoustid {

inline uint64_t packItem(uint32_t key, uint32_t value)
{
	return (uint64_t(key) << 32) | value;
}

inline uint32_t unpackItemKey(uint64_t item)
{
	return item >> 32;
}

inline uint32_t unpackItemValue(uint64_t item)
{
	return item & 0xFFFFFFFF;
}

}

#endif
