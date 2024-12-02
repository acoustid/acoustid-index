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

Running server:

    zig build run -- --dir /tmp/fpindex --port 8080 --log-level debug

Running unit tests:

    zig build test --summary all

Running integration tests:

    pytest -vv tests/

## HTTP API

### Get index

Returns information about an index.

```
GET /:indexname
```

### Create index

Creates a new index.

```
PUT /:indexname
```

### Delete index

Deletes an index.

```
DELETE /:indexname
```

### Update

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

### Search

Searches for a fingerprint in the index.

```
POST /:indexname/_search
```

```json
{"query": [100, 200, 300], "timeout": 10}
```
