// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_BASE_INDEX_H_
#define ACOUSTID_INDEX_BASE_INDEX_H_

#include <QString>
#include <QVector>

#include <variant>

#include "collector.h"

namespace Acoustid {

class BaseIndexTransaction {
 public:
    BaseIndexTransaction() {}
    virtual ~BaseIndexTransaction() {}

    virtual bool insertOrUpdateDocument(uint32_t docId, const QVector<uint32_t> &terms) = 0;
    virtual bool deleteDocument(uint32_t docId);

    virtual bool hasAttribute(const QString &name) = 0;
    virtual QString getAttribute(const QString &name) = 0;
    virtual void setAttribute(const QString &name, const QString &value) = 0;

    virtual void commit() = 0;
};

enum OpType {
    INVALID_OP = 0,
    INSERT_OR_UPDATE_DOCUMENT = 1,
    DELETE_DOCUMENT = 2,
    SET_ATTRIBUTE = 3,
};

struct InsertOrUpdateDocument {
    uint32_t docId;
    QVector<uint32_t> terms;
    InsertOrUpdateDocument(uint32_t docId, const QVector<uint32_t> &terms)
        : docId(docId), terms(terms) {}
};

struct DeleteDocument {
    uint32_t docId;
    DeleteDocument(uint32_t docId) : docId(docId) {}
};

struct SetAttribute {
    QString name;
    QString value;
    SetAttribute(const QString &name, const QString &value)
        : name(name), value(value) {}
};

typedef std::variant<std::monostate, InsertOrUpdateDocument, DeleteDocument, SetAttribute> OpData;

class Op {
 public:
    Op() : m_type(INVALID_OP) {}
    Op(InsertOrUpdateDocument data) : m_type(INSERT_OR_UPDATE_DOCUMENT), m_data(data) {}
    Op(DeleteDocument data) : m_type(DELETE_DOCUMENT), m_data(data) {}
    Op(SetAttribute data) : m_type(SET_ATTRIBUTE), m_data(data) {}

    OpType type() const { return m_type; }
    const OpData data() const { return m_data; }

    bool isValid() const { return m_type != INVALID_OP; }

 private:
    Op(OpType type, OpData data) : m_type(type), m_data(data) { }

    OpType m_type;
    OpData m_data;
};

class OpBatch {
 public:
    typedef QVector<Op>::iterator iterator;
    typedef QVector<Op>::const_iterator const_iterator;

    void insertOrUpdateDocument(uint32_t docId, const QVector<uint32_t> &terms) {
        m_ops.append(Op(InsertOrUpdateDocument(docId, terms)));
    }

    void deleteDocument(uint32_t docId) {
        m_ops.append(Op(DeleteDocument(docId)));
    }

    void setAttribute(const QString &name, const QString &value) {
        m_ops.append(Op(SetAttribute(name, value)));
    }

    void clear() {
        m_ops.clear();
    }

    size_t size() const { return m_ops.size(); }

    const_iterator begin() const {
        return m_ops.begin();
    }

    const_iterator end() const {
        return m_ops.end();
    }

 private:
    QVector<Op> m_ops;
};

class BaseIndex {
 public:
    BaseIndex() {}
    virtual ~BaseIndex() {}
    
	virtual void search(const QVector<uint32_t> &terms, Collector *collector, int64_t timeoutInMSecs) = 0;

    virtual bool hasAttribute(const QString &name) = 0;
    virtual QString getAttribute(const QString &name) = 0;

    virtual void applyUpdates(const OpBatch &ops) = 0;
};

} // namespace Acoustid

#endif // ACOUSTID_INDEX_BASE_INDEX_H_
