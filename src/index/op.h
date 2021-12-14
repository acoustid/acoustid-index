// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_INDEX_OP_H_
#define ACOUSTID_INDEX_INDEX_OP_H_

#include <QJsonObject>
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

    QJsonObject toJson() const;
    static InsertOrUpdateDocument fromJson(const QJsonObject &obj);

    bool operator==(const InsertOrUpdateDocument &other) const { return docId == other.docId && terms == other.terms; }

    bool operator!=(const InsertOrUpdateDocument &other) const { return !(*this == other); }

 private:
    InsertOrUpdateDocument() = default;
};

struct DeleteDocument {
    uint32_t docId;
    DeleteDocument(uint32_t docId) : docId(docId) {}

    QJsonObject toJson() const;
    static DeleteDocument fromJson(const QJsonObject &obj);

    bool operator==(const DeleteDocument &other) const { return docId == other.docId; }

    bool operator!=(const DeleteDocument &other) const { return !(*this == other); }

 private:
    DeleteDocument() = default;
};

struct SetAttribute {
    QString name;
    QString value;
    SetAttribute(const QString &name, const QString &value) : name(name), value(value) {}

    QJsonObject toJson() const;
    static SetAttribute fromJson(const QJsonObject &obj);

    bool operator==(const SetAttribute &other) const { return name == other.name && value == other.value; }

    bool operator!=(const SetAttribute &other) const { return !(*this == other); }

 private:
    SetAttribute() = default;
};

typedef std::variant<InsertOrUpdateDocument, DeleteDocument, SetAttribute> OpData;

class Op {
 public:
    Op(InsertOrUpdateDocument data) : m_type(INSERT_OR_UPDATE_DOCUMENT), m_data(data) {}
    Op(DeleteDocument data) : m_type(DELETE_DOCUMENT), m_data(data) {}
    Op(SetAttribute data) : m_type(SET_ATTRIBUTE), m_data(data) {}

    OpType type() const { return m_type; }
    const OpData &data() const { return m_data; }

    QJsonObject toJson() const;
    static Op fromJson(const QJsonObject &obj);

    bool operator==(const Op &other) const { return m_type == other.m_type && m_data == other.m_data; }

    bool operator!=(const Op &other) const { return !(*this == other); }

 private:
    Op(OpType type, OpData data) : m_type(type), m_data(data) {}

    OpType m_type;
    OpData m_data;
};

class OpBatch {
 public:
    typedef std::vector<Op>::iterator iterator;
    typedef std::vector<Op>::const_iterator const_iterator;

    void add(const Op &op) { m_ops.push_back(op); }

    void insertOrUpdateDocument(uint32_t docId, const std::vector<uint32_t> &terms) {
        m_ops.emplace_back(InsertOrUpdateDocument(docId, terms));
    }

    void deleteDocument(uint32_t docId) { m_ops.emplace_back(DeleteDocument(docId)); }

    void setAttribute(const QString &name, const QString &value) { m_ops.emplace_back(SetAttribute(name, value)); }
    QString getAttribute(const QString &name, const QString &defaultValue = QString()) const;

    void clear() { m_ops.clear(); }

    size_t size() const { return m_ops.size(); }

    const_iterator begin() const { return m_ops.begin(); }
    const_iterator end() const { return m_ops.end(); }

 private:
    std::vector<Op> m_ops;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_INDEX_OP_H_
