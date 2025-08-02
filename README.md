# Next generation of AcoustID fingerprint index

## Key changes from the first version

- Written in Zig instead of C++
- Supports updates and deletes
- HTTP API that can be used for managing multiple indexes
- Fast and concurrent write operations using WAL and in-memory segments
- Simplified internal file format, using msgpack for serialization wherever possible

## Building

Building from source code:

    zig build

Running tests:

    zig build test --summary all

Running server:

    zig build run -- --dir /tmp/fpindex --port 8080 --log-level debug

## HTTP API

The API supports both JSON and MessagePack formats. Use `Content-Type: application/json` or `Content-Type: application/vnd.msgpack` for requests, and `Accept: application/json` or `Accept: application/vnd.msgpack` for responses. JSON is the default if no headers are specified.

### Index Management

#### Check if index exists

Returns HTTP status 200 if the index exists, 404 if not found.

```http
HEAD /:indexname
```

#### Get index information

Returns detailed information about an index including version, segment count, document count, and attributes.

```http
GET /:indexname
```

**Response:**
```json
{
  "version": 1,
  "segments": 2,
  "docs": 1000,
  "attributes": {
    "min_document_id": 1,
    "max_document_id": 999
  }
}
```

#### Create index

Creates a new index. Returns empty JSON object `{}` on success.

```http
PUT /:indexname
```

#### Delete index

Deletes an index and all its data. Returns empty JSON object `{}` on success.

```http
DELETE /:indexname
```

#### Get segment information

Returns detailed information about index segments (memory and file segments).

```http
GET /:indexname/_segments
```

**Response:**
```json
{
  "segments": [
    {
      "kind": "memory",
      "version": 1,
      "merges": 0,
      "min_doc_id": 1,
      "max_doc_id": 100
    },
    {
      "kind": "file", 
      "version": 2,
      "merges": 1,
      "min_doc_id": 101,
      "max_doc_id": 999
    }
  ]
}
```

### Fingerprint Operations

#### Bulk update operations

Performs multiple insert/delete operations atomically. This is the preferred method for bulk operations.

```http
POST /:indexname/_update
```

**Request:**
```json
{
  "changes": [
    {"insert": {"id": 12345, "hashes": [100, 200, 300, 400, 500]}},
    {"insert": {"id": 67890, "hashes": [150, 250, 350]}},
    {"delete": {"id": 11111}}
  ]
}
```

**Response:** Empty JSON object `{}`

#### Search for fingerprints

Searches for fingerprints by finding documents that contain subsets of the query hashes. Returns results ranked by the number of matching hashes (score).

```http
POST /:indexname/_search
```

**Request:**
```json
{
  "query": [100, 200, 300, 400, 500],
  "timeout": 1000,
  "limit": 20
}
```

**Parameters:**
- `query` (required): Array of 32-bit unsigned integers representing hash values
- `timeout` (optional): Search timeout in milliseconds (default: 500, max: 10000)
- `limit` (optional): Maximum number of results to return (default: 40, min: 1, max: 100)

**Response:**
```json
{
  "results": [
    {"id": 12345, "score": 5},
    {"id": 67890, "score": 3}
  ]
}
```

**Score calculation:** The score represents the number of query hashes that match hashes in the fingerprint. Higher scores indicate better matches.

**Minimum score threshold:** Results must have at least `(query_length + 19) / 20` matching hashes and at least 10% of the query hashes.

#### Check if fingerprint exists

Returns HTTP status 200 if the fingerprint exists, 404 if not found.

```http
HEAD /:indexname/:fpid
```

#### Get fingerprint information

Returns metadata about a fingerprint. Note: Original hashes cannot be retrieved as they are stored in an optimized format.

```http
GET /:indexname/:fpid
```

**Response:**
```json
{
  "version": 1
}
```

#### Insert/update single fingerprint

Inserts or updates a single fingerprint. For bulk operations, prefer using `/_update`.

```http
PUT /:indexname/:fpid
```

**Request:**
```json
{
  "hashes": [100, 200, 300, 400, 500]
}
```

**Response:** Empty JSON object `{}`

#### Delete single fingerprint

Deletes a single fingerprint. For bulk operations, prefer using `/_update`.

```http
DELETE /:indexname/:fpid
```

**Response:** Empty JSON object `{}`

### System Utilities

#### Health check

Returns "OK" if the service is healthy.

```http
GET /_health
```

#### Index health check

Returns "OK" if the specific index is ready and healthy.

```http
GET /:indexname/_health
```

#### Prometheus metrics

Returns metrics in Prometheus format for monitoring.

```http
GET /_metrics
```

**Example metrics:**
```
# TYPE aindex_search_hits_total counter
aindex_search_hits_total 1250
# TYPE aindex_search_misses_total counter  
aindex_search_misses_total 45
# TYPE aindex_search_duration_seconds histogram
aindex_search_duration_seconds_bucket{le="0.005"} 100
```

### Error Responses

All endpoints return structured error responses in case of failures:

```json
{
  "error": "IndexNotFound"
}
```

**Common error codes:**
- `400 Bad Request`: Invalid request body, missing parameters, or malformed data
- `404 Not Found`: Index or fingerprint not found
- `415 Unsupported Media Type`: Invalid Content-Type header
- `500 Internal Server Error`: Server-side processing error
- `503 Service Unavailable`: Index not ready (still loading)
