# CLAUDE.md

## What this is

Ground-up rewrite of the AcoustID fpindex (inverted index for audio fingerprints:
ID + set of hashes; search = find IDs whose hash set intersects the query). Runs as an
HTTP service. The old implementation lives at `../acoustid/idx` (git history is the
reference/"answer key" for ported logic).

**Design of record: `notes/design-notes-2026-07.md`.** Read it before touching
replication, storage, or async questions — the architecture was decided there and
should not be re-litigated. Update that file (and the memory pointer) if a decision
changes.

## Key decisions (see notes for the why)

- **Stack**: Zig 0.16 + [zio](../zio) (async runtime, own project) + [dusty](../dusty)
  (HTTP, own project) + [msgpack.zig](../msgpack.zig). All local path deps for now.
- **No backwards compatibility of any kind.** No v1 segment/oplog/snapshot readers, no
  converters. Migration = rebuild nodes from the log. This deliberately overrides the
  old project's "don't break backwards compat" rule.
- **Storage**: fully RAM-resident segments (load into anonymous memory at startup via
  io_uring, mlock, MADV_HUGEPAGE), no mmap. Hard invariant: the index fits in RAM.
- **Replication**: ordered log with two implementations behind one interface —
  file-backed (standalone mode, authoritative log) and PostgreSQL changelog (cluster
  mode). Same consumer/recovery/snapshot pipeline both ways. No NATS.
- **Two zio runtimes**: search/API (read-only) and background (consumer/checkpoint/merge).

## Port buckets (from old `../acoustid/idx/src`)

- Port nearly verbatim (pure, tested, no I/O): segment, block, streamvbyte (pure Zig SIMD),
  segment_merge_policy (Lucene-derived, subtle — never rewrite from memory), scoring in
  common, Metadata.
- Reimplement to new design: filefmt (v2), FileSegment (heap slice not mmap),
  MemorySegment, segment lists / SharedPtr snapshots, manifest, the file-log impl.
- Drop entirely: old server/httpz, NATS/ClusterMultiIndex, thread Scheduler/WaitGroup.

Discipline: don't redesign while transplanting — improvements go into the notes, not
into the port.

## Build and test

    zig build                 # build ./zig-out/bin/fpindex
    zig build run             # run the server
    zig build unit-tests      # unit tests

## Agent instructions

- Pragmatic tone, not enterprise. Run unit tests after code changes.
- Answer questions before changing code.
