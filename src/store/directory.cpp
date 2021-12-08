// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "directory.h"

using namespace Acoustid;

Directory::~Directory() {}

bool Directory::fileExists(const QString& name) {
    QStringList names = listFiles();
    for (size_t i = 0; i < names.size(); i++) {
        if (names.at(i) == name) {
            return true;
        }
    }
    return false;
}

void Directory::sync(const QStringList& names) {
    // noop
}
