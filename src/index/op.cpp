#include "op.h"

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
        output[i] = array.at(i).toVariant().toUInt();
    }
}

QJsonObject InsertOrUpdateDocument::toJson() const {
    QJsonObject json{
        {"id", toJsonValue(docId)},
        {"terms", toJsonArray(terms.begin(), terms.end())},
    };
    return json;
}

InsertOrUpdateDocument InsertOrUpdateDocument::fromJson(const QJsonObject &json) {
    InsertOrUpdateDocument op;
    fromJsonValue(json["id"], op.docId);
    fromJsonArray(json["terms"].toArray(), op.terms);
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
    switch (m_type) {
        case INSERT_OR_UPDATE_DOCUMENT:
            json["upsert"] = std::get<InsertOrUpdateDocument>(m_data).toJson();
            break;
        case DELETE_DOCUMENT:
            json["delete"] = std::get<DeleteDocument>(m_data).toJson();
            break;
        case SET_ATTRIBUTE:
            json["set"] = std::get<SetAttribute>(m_data).toJson();
            break;
    }
    return json;
};

Op Op::fromJson(const QJsonObject &json) {
    for (auto it = json.begin(); it != json.end(); ++it) {
        if (it.key() == "upsert") {
            return Op(InsertOrUpdateDocument::fromJson(it.value().toObject()));
        } else if (it.key() == "delete") {
            return Op(DeleteDocument::fromJson(it.value().toObject()));
        } else if (it.key() == "set") {
            return Op(SetAttribute::fromJson(it.value().toObject()));
        }
    }
    throw Exception("invalid operation");
}

}  // namespace Acoustid
