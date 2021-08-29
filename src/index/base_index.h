// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_BASE_INDEX_H_
#define ACOUSTID_INDEX_BASE_INDEX_H_

#include <QString>
#include <QVector>

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
    INSERT_OR_UPDATE_DOCUMENT = 1,
    DELETE_DOCUMENT = 2,
    SET_ATTRIBUTE = 3,
};

struct InsertOrUpdateDocumentData {
    uint32_t docId;
    QVector<uint32_t> terms;
};

struct DeleteDocumentData {
    uint32_t docId;
};

struct SetAttributeData {
    QString name;
    QString value;
};

union OpData {
    InsertOrUpdateDocumentData *insertOrUpdateDocument;
    DeleteDocumentData *deleteDocument;
    SetAttributeData *setAttribute;
};

class Op {
 public:
    ~Op() {
        switch (m_type) {
            case INSERT_OR_UPDATE_DOCUMENT:
                delete m_data.insertOrUpdateDocument;
            case DELETE_DOCUMENT:
                delete m_data.deleteDocument;
            case SET_ATTRIBUTE:
                delete m_data.setAttribute;
        }
    }

    OpType type() const { return m_type; }
    const OpData *data() const { return &m_data; }

    static Op *insertOrUpdateDocument(uint32_t docId, const QVector<uint32_t> &terms) {
        auto d = new InsertOrUpdateDocumentData();
        d->docId = docId;
        d->terms = terms;
        return new Op(INSERT_OR_UPDATE_DOCUMENT, OpData { .insertOrUpdateDocument = d });
    }

 private:
    Op(OpType type, OpData data) : m_type(type), m_data(data) { }

    OpType m_type;
    OpData m_data;
};

class OpStream {
 public:
    virtual ~OpStream() {}

    virtual bool isValid() = 0;
    virtual bool next() = 0;

    virtual Op *operation() = 0;
};

class BaseIndex {
 public:
    BaseIndex() {}
    virtual ~BaseIndex() {}
    
	virtual void search(const QVector<uint32_t> &terms, Collector *collector, int64_t timeoutInMSecs) = 0;

    virtual bool hasAttribute(const QString &name) = 0;
    virtual QString getAttribute(const QString &name) = 0;

    virtual void applyUpdates(OpStream *updates) = 0;
};

} // namespace Acoustid

#endif // ACOUSTID_INDEX_BASE_INDEX_H_
