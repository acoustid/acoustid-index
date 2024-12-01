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

## Using via HTTP API

Create index:

    curl -XPUT -v http://localhost:8080/1/_update

Add a fingerprint:

    curl -XPOST -d '{"changes": [{"insert": {"id": 1, "hashes": [1,2,3]}}]}' -v http://localhost:8080/1/_update

Delete a fingerprint:

    curl -XPOST -d '{"changes": [{"delete": {"id": 2}}]}' -v http://localhost:8080/1/_update

Search for a fingerprint:

    curl -XPOST -d '{"query": [1,2,3], "timeout": 10}' -v http://localhost:8080/_search
