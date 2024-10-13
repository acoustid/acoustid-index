# Experimental version of AcoustID index

Building from source code:

    zig build

Running tests:

    zig build test --summary all

Adding document:

    curl -XPOST -d '{"changes": [{"insert": {"id": 1, "hashes": [1,2,3]}}]}' -v http://localhost:8080/_update

Searching:

    curl -XPOST -d '{"query": [1,2,3], "timeout": 10}' -v http://localhost:8080/_search
