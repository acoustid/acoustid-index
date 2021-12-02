// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_BASE_INDEX_H_
#define ACOUSTID_INDEX_BASE_INDEX_H_

#include <QString>
#include <variant>
#include <vector>

namespace Acoustid {

enum OpType {
    INSERT_OR_UPDATE_DOCUMENT = 1,
    DELETE_DOCUMENT = 2,
    SET_ATTRIBUTE = 3,
};

struct InsertOrUpdateDocument {
    uint32_t docId;
    std::vector<uint32_t> terms;
    InsertOrUpdateDocument(uint32_t docId, const std::vector<uint32_t> &terms) : docId(docId), terms(terms) {}
};

struct DeleteDocument {
    uint32_t docId;
    DeleteDocument(uint32_t docId) : docId(docId) {}
};

struct SetAttribute {
    QString name;
    QString value;
    SetAttribute(const QString &name, const QString &value) : name(name), value(value) {}
};

typedef std::variant<InsertOrUpdateDocument, DeleteDocument, SetAttribute> OpData;

class Op {
 public:
    Op(InsertOrUpdateDocument data) : m_type(INSERT_OR_UPDATE_DOCUMENT), m_data(data) {}
    Op(DeleteDocument data) : m_type(DELETE_DOCUMENT), m_data(data) {}
    Op(SetAttribute data) : m_type(SET_ATTRIBUTE), m_data(data) {}

    OpType type() const { return m_type; }
    const OpData &data() const { return m_data; }

 private:
    Op(OpType type, OpData data) : m_type(type), m_data(data) {}

    OpType m_type;
    OpData m_data;
};

class OpBatch {
 public:
    typedef std::vector<Op>::iterator iterator;
    typedef std::vector<Op>::const_iterator const_iterator;

    void insertOrUpdateDocument(uint32_t docId, const std::vector<uint32_t> &terms) {
        m_ops.push_back(Op(InsertOrUpdateDocument(docId, terms)));
    }

    void deleteDocument(uint32_t docId) { m_ops.push_back(Op(DeleteDocument(docId))); }

    void setAttribute(const QString &name, const QString &value) { m_ops.push_back(Op(SetAttribute(name, value))); }

    void clear() { m_ops.clear(); }

    size_t size() const { return m_ops.size(); }

    const_iterator begin() const { return m_ops.begin(); }
    const_iterator end() const { return m_ops.end(); }

 private:
    std::vector<Op> m_ops;
};

class SearchResult {
 public:
    SearchResult(uint32_t docId, uint32_t score) : m_docId(docId), m_score(score) {}

    uint32_t docId() const { return m_docId; }
    uint32_t score() const { return m_score; }

    bool operator==(const SearchResult &other) const { return m_docId == other.m_docId && m_score == other.m_score; }
    bool operator!=(const SearchResult &other) const { return !operator==(other); }

 private:
    uint32_t m_docId;
    uint32_t m_score;
};

class BaseIndex {
 public:
    BaseIndex() {}
    virtual ~BaseIndex() {}

    virtual bool containsDocument(uint32_t docId) = 0;
    virtual std::vector<SearchResult> search(const std::vector<uint32_t> &terms, int64_t timeoutInMSecs) = 0;

    virtual bool hasAttribute(const QString &name) = 0;
    virtual QString getAttribute(const QString &name) = 0;

    virtual void applyUpdates(const OpBatch &ops) = 0;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_BASE_INDEX_H_
