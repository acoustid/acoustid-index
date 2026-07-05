# fpindex (next generation)

Inverted index for searching audio fingerprints (an ID plus a set of hashes). Search
finds fingerprint IDs whose hash set intersects the query. Runs as an HTTP service.

Ground-up rewrite on Zig 0.16 with [zio](../zio) (async runtime), [dusty](../dusty)
(HTTP), and [msgpack.zig](../msgpack.zig). Fully RAM-resident storage, an ordered log
for replication (file-backed standalone, or PostgreSQL for a cluster). See
`notes/design-notes-2026-07.md` for the architecture and `CLAUDE.md` for the plan.

Status: early. Walking skeleton — HTTP server with `/_health`.

## Build

    zig build              # -> ./zig-out/bin/fpindex
    zig build run          # run it
    zig build unit-tests   # unit tests

## Try it

    ./zig-out/bin/fpindex &
    curl http://127.0.0.1:8080/_health   # -> OK
