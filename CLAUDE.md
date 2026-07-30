# CLAUDE.md

## What this is

AcoustID fpindex: inverted index for audio fingerprints (ID + set of hashes;
search = find IDs whose hash set intersects the query). Runs as an HTTP service,
standalone or as a cluster. The previous implementation lives at `../acoustid/idx`.

**`README.md` is the documentation** — architecture, replication, bootstrap, API,
operations. Keep it current when behavior changes; it is reference documentation,
not a diary — no design justifications there. The decisions below are settled and
should not be re-litigated; the why lives in git and PR history.

## Key decisions

- **Stack**: Zig 0.16 + [zio](../zio) (async runtime, own project) + [dusty](../dusty)
  (HTTP, own project) + [msgpack.zig](../msgpack.zig). All local path deps for now.
- **No backwards compatibility with the previous implementation.** No v1
  segment/oplog/snapshot readers, no converters. Migration = rebuild nodes from
  the log.
- **Storage**: fully RAM-resident segments (load into anonymous memory at startup via
  io_uring, mlock, MADV_HUGEPAGE), no mmap. Hard invariant: the index fits in RAM.
- **Replication**: ordered log with two implementations behind one interface —
  file-backed (standalone mode, authoritative log) and PostgreSQL changelog (cluster
  mode). Same consumer/recovery/snapshot pipeline both ways. No NATS.
- **Two zio runtimes**: search/API (read-only) and background (consumer/checkpoint/merge).
- **segment_merge_policy is Lucene-derived and subtle** — never rewrite it from memory.

## Build and test

    zig build                 # build ./zig-out/bin/fpindex
    zig build run             # run the server
    zig build unit-tests      # unit tests

## Agent instructions

- Pragmatic tone, not enterprise. Run unit tests after code changes.
- Answer questions before changing code.
