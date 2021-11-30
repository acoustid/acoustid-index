// Copyright (C) 2020  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_SERVER_ERRORS_H_
#define ACOUSTID_SERVER_ERRORS_H_

#include "util/exceptions.h"

namespace Acoustid {
namespace Server {

class ProtocolException : public Exception {
 public:
    ProtocolException(const QString& msg) : Exception(msg) {}
};

class BadRequest : public ProtocolException {
 public:
    BadRequest(const QString& msg) : ProtocolException(msg) {}
};

class HandlerException : public Exception {
 public:
    HandlerException(const QString& msg) : Exception(msg) {}
};

class NotInTransactionException : public HandlerException {
 public:
    NotInTransactionException() : HandlerException("not in transaction") {}
};

class AlreadyInTransactionException : public HandlerException {
 public:
    AlreadyInTransactionException() : HandlerException("already in transaction") {}
};

}  // namespace Server
}  // namespace Acoustid

#endif
