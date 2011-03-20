// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_UTIL_TEST_UTILS_H_
#define ACOUSTID_UTIL_TEST_UTILS_H_

#include <ostream>
#include <QString>

#define ASSERT_INTARRAY_EQ(v1, v2, length) \
	for (size_t i = 0; i < (length); i++) { \
		ASSERT_EQ((long long)(v1)[i], (long long)(v2)[i]) << "Different value at index " << i; \
	} 

inline std::ostream& operator <<(std::ostream& os, const QString& s) {
  return os << qPrintable(s);
}

#endif

