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

### Index management

#### Check if index exists

Returns HTTP status 200 if the index exists.

```
HEAD /:indexname
```

#### Get index info

Returns information about an index.

```
GET /:indexname
```
#### Create index

Creates a new index.

```
PUT /:indexname
```

#### Delete index

Deletes an index.

```
DELETE /:indexname
```

### Fingerprint management

#### Update

Performs multiple operations on an index.

```
POST /:indexname/_update
```

```json
{
  "changes": [
    {"insert": {"id": 1, "hashes": [100, 200, 300]}},
    {"delete": {"id": 2}}
  ]
}
```

#### Search

Searches for a fingerprint in the index.

```
POST /:indexname/_search
```

```json
{"query": [100, 200, 300], "timeout": 10}
```

#### Check if fingerprint exists

Returns HTTP status 200 if the fingerprint exists.

```
HEAD /:indexname/:fpid
```

#### Get fingerprint info

Gets information about a fingeprint.

There is no way to get back the original hashes, they are not stored in a way that makes it possible to retrieve them.

```
GET /:indexname/:fpid
```

#### Update single fingerprint

Updates a single fingerprint.

Prefer using `/_update` for bulk operations.

```
PUT /:indexname/:fpid
```

```json
{"hashes": [100, 200, 300]}
```

#### Delete single fingerprint

Deletes a single fingerprint.

Prefer using `/_update` for bulk operations.

```
DELETE /:indexname/:fpid
```

### System utilities

#### Healhcheck

```
GET /_health
GET /:indexname/_health
```

#### Prometheus metrics

```
GET /_metrics
```
