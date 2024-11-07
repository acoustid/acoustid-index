#include "op.h"

#include <QDebug>
#include <QJsonArray>
#include <QJsonValue>
#include <QVariant>

#include "util/exceptions.h"

namespace Acoustid {

QJsonValue toJsonValue(uint32_t value) { return QJsonValue(static_cast<qint64>(value)); }

QJsonValue toJsonValue(const QString &value) { return QJsonValue(value); }

template <typename InputIt>
QJsonArray toJsonArray(InputIt begin, InputIt end) {
    QJsonArray array;
    for (auto it = begin; it != end; ++it) {
        array.append(toJsonValue(*it));
    }
    return array;
}

void fromJsonValue(const QJsonValue &value, uint32_t &output) { output = value.toVariant().toUInt(); }

void fromJsonValue(const QJsonValue &value, QString &output) { output = value.toString(); }

void fromJsonArray(const QJsonArray &array, std::vector<uint32_t> &output) {
    output.reserve(array.size());
    for (int i = 0; i < array.size(); ++i) {
        output.push_back(array.at(i).toVariant().toUInt());
    }
}

QJsonObject InsertOrUpdateDocument::toJson() const {
    QJsonObject json{
        {"id", toJsonValue(docId)},
        {"hashes", toJsonArray(terms.begin(), terms.end())},
    };
    return json;
}

InsertOrUpdateDocument InsertOrUpdateDocument::fromJson(const QJsonObject &json) {
    InsertOrUpdateDocument op;
    fromJsonValue(json["id"], op.docId);
    fromJsonArray(json["hashes"].toArray(), op.terms);
    return op;
}

QJsonObject DeleteDocument::toJson() const {
    QJsonObject json{
        {"id", toJsonValue(docId)},
    };
    return json;
}

DeleteDocument DeleteDocument::fromJson(const QJsonObject &json) {
    DeleteDocument op;
    fromJsonValue(json["id"], op.docId);
    return op;
}

QJsonObject SetAttribute::toJson() const {
    QJsonObject json{
        {"name", toJsonValue(name)},
        {"value", toJsonValue(value)},
    };
    return json;
}

SetAttribute SetAttribute::fromJson(const QJsonObject &json) {
    SetAttribute op;
    fromJsonValue(json["name"], op.name);
    fromJsonValue(json["value"], op.value);
    return op;
}

QJsonObject Op::toJson() const {
    QJsonObject json;
    switch (type()) {
        case INSERT_OR_UPDATE_DOCUMENT:
            json["upsert"] = data<InsertOrUpdateDocument>().toJson();
            break;
        case DELETE_DOCUMENT:
            json["delete"] = data<DeleteDocument>().toJson();
            break;
        case SET_ATTRIBUTE:
            json["set"] = data<SetAttribute>().toJson();
            break;
    }
    return json;
}

Op Op::fromJson(const QJsonObject &json) {
    for (auto it = json.begin(); it != json.end(); ++it) {
        if (it.key() == "insert") {
            return Op(InsertOrUpdateDocument::fromJson(it.value().toObject()));
        } else if (it.key() == "delete") {
            return Op(DeleteDocument::fromJson(it.value().toObject()));
        } else if (it.key() == "set") {
            return Op(SetAttribute::fromJson(it.value().toObject()));
        }
    }
    throw Exception("invalid operation");
}

QString OpBatch::getAttribute(const QString &name, const QString &defaultValue) const {
    auto value = defaultValue;
    for (const auto &op : m_ops) {
        if (op.type() == SET_ATTRIBUTE) {
            const auto &setAttribute = op.data<SetAttribute>();
            if (setAttribute.name == name) {
                value = setAttribute.value;
            }
        }
    }
    return value;
}

}  // namespace Acoustid
