#include "http.h"

#include <QtConcurrent>

#include "index/index.h"
#include "index/index_writer.h"
#include "index/multi_index.h"
#include "metrics.h"

using namespace qhttp;
using namespace qhttp::server;

namespace Acoustid {
namespace Server {

static HttpResponse handleMetricsRequest(const HttpRequest &request, const QSharedPointer<Metrics> &metrics) {
    auto content = metrics->toStringList().join("\n") + "\n";
    auto response = HttpResponse(HTTP_OK, content);
    response.setHeader("Content-Type", "text/plain; version=0.0.4");
    return response;
}

static HttpResponse makeJsonErrorResponse(HttpStatusCode status, const QString &type, const QString &description) {
    QJsonObject errorJson{
        {"error",
         QJsonObject{
             {"type", type},
             {"description", description},
         }},
        {"status", int(status)},
    };
    return HttpResponse(status, QJsonDocument(errorJson));
}

static HttpResponse errNotFound(const QString &description) {
    return makeJsonErrorResponse(HTTP_NOT_FOUND, "not_found", description);
}

static HttpResponse errNotImplemented(const QString &description) {
    return makeJsonErrorResponse(HTTP_INTERNAL_SERVER_ERROR, "not_implemented", description);
}

static HttpResponse errBadRequest(const QString &type, const QString &description) {
    return makeJsonErrorResponse(HTTP_BAD_REQUEST, type, description);
}

static HttpResponse errServiceUnavailable(const QString &description) {
    return makeJsonErrorResponse(HTTP_SERVICE_UNAVAILABLE, "service_unavailable", description);
}

static HttpResponse errInvalidParameter(const QString &description) {
    return errBadRequest("invalid_parameter", description);
}

static HttpResponse errInvalidTerms() { return errBadRequest("invalid_terms", "invalid terms"); }

static QString getIndexName(const HttpRequest &request) {
    auto indexName = request.param(":index");
    if (indexName.isEmpty()) {
        throw HttpResponseException(errInvalidParameter("missing index name"));
    }
    if (indexName.startsWith('_')) {
        throw HttpResponseException(errInvalidParameter("invalid index name"));
    }
    return indexName;
}

static uint32_t getDocId(const HttpRequest &request) {
    auto docIdStr = request.param(":docId");
    if (docIdStr.isEmpty()) {
        throw HttpResponseException(errInvalidParameter("missing document ID"));
    }
    auto docId = docIdStr.toUInt();
    if (docId == 0) {
        throw HttpResponseException(errInvalidParameter("invalid document ID"));
    }
    return docId;
}

static std::vector<uint32_t> parseTerms(const QString &terms) {
    std::vector<uint32_t> result;
    if (terms.isEmpty()) {
        return result;
    }
    QStringList parts = terms.split(',');
    for (const auto &part : parts) {
        bool ok;
        auto term = part.toUInt(&ok);
        if (!ok) {
            throw HttpResponseException(errInvalidTerms());
        }
        result.push_back(term);
    }
    return result;
}

static std::vector<uint32_t> parseTerms(const QJsonArray &values) {
    std::vector<uint32_t> result;
    for (const auto &value : values) {
        if (!value.isDouble()) {
            throw HttpResponseException(errInvalidTerms());
        }
        result.push_back(value.toVariant().toUInt());
    }
    return result;
}

static std::vector<uint32_t> parseTerms(const QJsonValue &value) {
    if (value.isArray()) {
        return parseTerms(value.toArray());
    } else if (value.isString()) {
        return parseTerms(value.toString());
    } else {
        throw HttpResponseException(errInvalidTerms());
    }
}

const QString MAIN_INDEX_NAME = "main";

static QSharedPointer<Index> getIndex(const HttpRequest &request, const QSharedPointer<MultiIndex> &index,
                                      bool create = false) {
    auto indexName = getIndexName(request);
    try {
        if (indexName == MAIN_INDEX_NAME) {
            return index->getRootIndex(create);
        } else {
            return index->getIndex(indexName, create);
        }
    } catch (const IndexNotFoundException &e) {
        throw HttpResponseException(errNotFound("index does not exist"));
    }
}

static HttpResponse handleHeadIndexRequest(const HttpRequest &request, const QSharedPointer<MultiIndex> &indexes) {
    getIndex(request, indexes, false);

    QJsonObject responseJson;
    return HttpResponse(HTTP_OK, QJsonDocument(responseJson));
}

static HttpResponse handleGetIndexRequest(const HttpRequest &request, const QSharedPointer<MultiIndex> &indexes) {
    auto index = getIndex(request, indexes, false);

    QJsonObject responseJson{
        {"revision", index->info().revision()},
    };
    return HttpResponse(HTTP_OK, QJsonDocument(responseJson));
}

static HttpResponse handlePutIndexRequest(const HttpRequest &request, const QSharedPointer<MultiIndex> &indexes) {
    return errNotImplemented("not implemented in this version of acoustid-index");
}

static HttpResponse handleDeleteIndexRequest(const HttpRequest &request, const QSharedPointer<MultiIndex> &indexes) {
    return errNotImplemented("not implemented in this version of acoustid-index");
}

static HttpResponse handleHeadDocumentRequest(const HttpRequest &request, const QSharedPointer<MultiIndex> &indexes) {
    return errNotImplemented("not implemented in this version of acoustid-index");
}

static HttpResponse handleGetDocumentRequest(const HttpRequest &request, const QSharedPointer<MultiIndex> &indexes) {
    return errNotImplemented("not implemented in this version of acoustid-index");
}

static HttpResponse handlePutDocumentRequest(const HttpRequest &request, const QSharedPointer<MultiIndex> &indexes) {
    auto index = getIndex(request, indexes);
    auto docId = getDocId(request);

    auto body = request.json().object();
    if (body.isEmpty()) {
        return errInvalidTerms();
    }
    auto terms = parseTerms(body.value("terms"));

    try {
        auto writer = index->openWriter(true, 1000);
        writer->addDocument(docId, terms.data(), terms.size());
        writer->commit();
    } catch (const IndexIsLocked &e) {
        return errServiceUnavailable("index is locked");
    }

    QJsonObject responseJson;
    return HttpResponse(HTTP_OK, QJsonDocument(responseJson));
}

static HttpResponse handleDeleteDocumentRequest(const HttpRequest &request, const QSharedPointer<MultiIndex> &indexes) {
    return errNotImplemented("not implemented in this version of acoustid-index");
}

// Handle search requests.
static HttpResponse handleSearchRequest(const HttpRequest &request, const QSharedPointer<MultiIndex> &indexes) {
    auto index = getIndex(request, indexes);

    auto query = parseTerms(request.param("query"));
    if (query.empty()) {
        return errInvalidParameter("query is empty");
    }

    auto limit = request.param("limit").toUInt();
    if (!limit) {
        limit = 100;
    }

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
    return HttpResponse(HTTP_OK, QJsonDocument(responseJson));
}

// Handle bulk requests.
static HttpResponse handleBulkRequest(const HttpRequest &request, const QSharedPointer<MultiIndex> &indexes) {
    auto index = getIndex(request, indexes);

    QJsonArray opsJsonArray;

    auto doc = request.json();
    if (doc.isObject()) {
        auto obj = doc.object();
        if (obj.contains("operations")) {
            auto value = obj.value("operations");
            if (value.isArray()) {
                opsJsonArray = value.toArray();
            } else {
                return errBadRequest("invalid_bulk_operation", "'operations' must be an array");
            }
        }
    } else if (doc.isArray()) {
        opsJsonArray = doc.array();
    } else {
        return errBadRequest("invalid_bulk_operation",
                             "request body must be either an array or an object with 'operations' key in it");
    }

    try {
        auto writer = index->openWriter(true, 1000);
        for (auto operation : opsJsonArray) {
            if (!operation.isObject()) {
                return errBadRequest("invalid_bulk_operation", "operation must be an object");
            }
            auto operationObj = operation.toObject();
            if (operationObj.contains("upsert")) {
                auto docObj = operationObj.value("upsert").toObject();
                auto docId = docObj.value("id").toInt();
                auto terms = parseTerms(docObj.value("terms"));
                writer->addDocument(docId, terms.data(), terms.size());
            }
            if (operationObj.contains("delete")) {
                return errNotImplemented("not implemented in this version of acoustid-index");
            }
            if (operationObj.contains("set")) {
                auto attrObj = operationObj.value("set").toObject();
                auto name = attrObj.value("name").toString();
                auto value = attrObj.value("value").toString();
                writer->setAttribute(name, value);
            }
        }
        writer->commit();
    } catch (const IndexIsLocked &e) {
        return errServiceUnavailable("index is locked");
    }

    QJsonObject responseJson;
    return HttpResponse(HTTP_OK, QJsonDocument(responseJson));
}

HttpRequestHandler::HttpRequestHandler(QSharedPointer<MultiIndex> indexes, QSharedPointer<Metrics> metrics)
    : m_indexes(indexes), m_metrics(metrics) {
    // Healthchecks
    m_router.route(HTTP_GET, "/_health/alive", [=](auto req) {
        return HttpResponse(HTTP_OK, "OK\n");
    });
    m_router.route(HTTP_GET, "/_health/ready", [=](auto req) {
        return HttpResponse(HTTP_OK, "OK\n");
    });

    // Prometheus metrics
    m_router.route(HTTP_GET, "/_metrics", [=](auto req) {
        return handleMetricsRequest(req, m_metrics);
    });

    // Document API
    m_router.route(HTTP_HEAD, "/:index/_doc/:docId", [=](auto req) {
        return handleHeadDocumentRequest(req, m_indexes);
    });
    m_router.route(HTTP_GET, "/:index/_doc/:docId", [=](auto req) {
        return handleGetDocumentRequest(req, m_indexes);
    });
    m_router.route(HTTP_PUT, "/:index/_doc/:docId", [=](auto req) {
        return handlePutDocumentRequest(req, m_indexes);
    });
    m_router.route(HTTP_DELETE, "/:index/_doc/:docId", [=](auto req) {
        return handleDeleteDocumentRequest(req, m_indexes);
    });

    // Bulk API
    m_router.route(HTTP_POST, "/:index/_bulk", [=](auto req) {
        return handleBulkRequest(req, m_indexes);
    });

    // Search API
    m_router.route(HTTP_GET, "/:index/_search", [=](auto req) {
        return handleSearchRequest(req, m_indexes);
    });

    // Index API
    m_router.route(HTTP_HEAD, "/:index", [=](auto req) {
        return handleHeadIndexRequest(req, m_indexes);
    });
    m_router.route(HTTP_GET, "/:index", [=](auto req) {
        return handleGetIndexRequest(req, m_indexes);
    });
    m_router.route(HTTP_PUT, "/:index", [=](auto req) {
        return handlePutIndexRequest(req, m_indexes);
    });
    m_router.route(HTTP_DELETE, "/:index", [=](auto req) {
        return handleDeleteIndexRequest(req, m_indexes);
    });
}

}  // namespace Server
}  // namespace Acoustid
