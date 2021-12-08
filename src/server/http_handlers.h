#ifndef ACOUSTID_INDEX_SERVER_HTTP_HANDLERS_H_
#define ACOUSTID_INDEX_SERVER_HTTP_HANDLERS_H_

#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>

#include "http.h"
#include "index/index.h"
#include "index/multi_index.h"
#include "qhttpabstracts.hpp"

namespace Acoustid {
namespace Server {

template <typename T>
inline HttpResponse makeResponse(qhttp::TStatusCode status, const T &body) {
    HttpResponse response;
    response.setStatus(status);
    response.setBody(body);
    return response;
}

inline HttpResponse makeErrorResponse(qhttp::TStatusCode status, const QString &errorType) {
    QJsonObject responseJson{
        {"error",
         QJsonObject{
             {"type", errorType},
         }},
        {"status", int(status)},
    };
    return makeResponse(status, QJsonDocument(responseJson));
}

class JsonHttpException : public HttpResponseException {
 public:
    JsonHttpException(qhttp::TStatusCode status, const QString &errorType)
        : HttpResponseException(makeErrorResponse(status, errorType)) {}
};

class BadRequestException : public JsonHttpException {
 public:
    BadRequestException(const QString &errorType) : JsonHttpException(qhttp::ESTATUS_BAD_REQUEST, errorType) {}
};

class NotFoundException : public JsonHttpException {
 public:
    NotFoundException(const QString &errorType) : JsonHttpException(qhttp::ESTATUS_NOT_FOUND, errorType) {}
};

template <typename T>
HttpResponse handleIndexRequest(const HttpRequest &request, T &&handler, const QSharedPointer<MultiIndex> &indexes) {
    auto indexName = request.param(":index");
    try {
        auto index = indexes->getIndex(indexName);
        return handler(request, index);
    } catch (const IndexNotFoundException &ex) {
        throw NotFoundException("index_not_found");
    }
}

inline HttpResponse handleGetDocumentRequest(const HttpRequest &request, const QSharedPointer<Index> &index) {
    auto docId = request.param(":id").toUInt();
    if (!index->containsDocument(docId)) {
        throw NotFoundException("document_not_found");
    }
    QJsonObject responseJson{
        {"id", qint64(docId)},
    };
    return makeResponse(qhttp::ESTATUS_OK, QJsonDocument(responseJson));
}

inline HttpResponse handleCreateDocumentRequest(const HttpRequest &request, const QSharedPointer<Index> &index) {
    auto docId = request.param(":id").toUInt();
    if (docId == 0) {
        throw BadRequestException("invalid_document_id");
    }
    auto body = request.json().object();
    if (body.isEmpty()) {
        throw BadRequestException("invalid_document");
    }
    auto terms = parseTerms(body.value("terms"));
    OpBatch batch;
    batch.insertOrUpdateDocument(docId, terms);
    index->applyUpdates(batch);
    QJsonObject responseJson{
        {"id", qint64(docId)},
    };
    return makeResponse(qhttp::ESTATUS_OK, QJsonDocument(responseJson));
}

inline HttpResponse handleDeleteDocumentRequest(const HttpRequest &request, const QSharedPointer<Index> &index) {
    auto docId = request.param(":id").toUInt();
    if (docId == 0) {
        throw BadRequestException("invalid_document_id");
    }
    OpBatch batch;
    batch.deleteDocument(docId);
    index->applyUpdates(batch);
    QJsonObject responseJson{};
    return makeResponse(qhttp::ESTATUS_OK, QJsonDocument(responseJson));
}

inline HttpResponse handleSearchRequest(const HttpRequest &request, const QSharedPointer<Index> &index) {
    auto query = parseTerms(request.param("query"));
    if (query.empty()) {
        throw BadRequestException("invalid_query");
    }

    auto limit = request.param("limit").toInt();
    auto results = index->search(query);
    filterSearchResults(results, limit);
    QJsonArray resultsJson;
    for (auto &result : results) {
        resultsJson.append(QJsonObject{
            {"id", qint64(result.docId())},
            {"score", result.score()},
        });
    }
    QJsonObject responseJson{
        {"results", resultsJson},
    };
    return makeResponse(qhttp::ESTATUS_OK, QJsonDocument(responseJson));
}

}  // namespace Server
}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_SERVER_HTTP_HANDLERS_H_
