// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include "directory.h"

using namespace Acoustid;

Directory::~Directory() {}

bool Directory::fileExists(const QString& name) {
    return listFiles().contains(name);
}

void Directory::sync(const QStringList& names) {
    // noop
}
