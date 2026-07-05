# Design notes: replication + fully-resident storage + zio migration

Working notes from design discussion, 2026-07-05.

## Goals / invariants

- 100M fingerprints (~150 hashes each), search <50ms (target: low ms), <=64GB RAM per node.
- N identical replicas with the same data, real-time updates, cheap to run.
- **Hard invariant: the index always fits in RAM.** The whole design depends on it.
  Enforce it: configured memory budget per node, account segments + merge transient
  headroom against it, refuse startup/merge when exceeded. Alert on locked bytes vs budget.

## Replication architecture

### Decision

Ordered operation log in PostgreSQL (Patroni for HA). No NATS. No fpindex primary,
no election, no node-to-node protocol. Every fpindex node is an identical log consumer.

Why: the index is a deterministic function of an ordered log, so same log prefix =
same logical index. The only thing that needs consensus is write ordering, and PG+Patroni
already solves that. Native fpindex primary->replica replication would force us to
reimplement epochs/timelines (diverged old primary after failover = the PG timeline
rollback problem, but in our code).

### Changelog table (PG is the write interface)

```sql
CREATE TABLE fpindex_changelog (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    index_name  text NOT NULL,
    op          smallint NOT NULL,     -- insert / delete / set_metadata
    fp_id       bigint,
    hashes      bytea                  -- raw LE u32s
) PARTITION BY RANGE (id);
```

- Raw columns, not msgpack blobs: writers INSERT changelog rows **in the same PG
  transaction** as their own source-of-truth writes -> no dual-write problem, no gaps
  (fixes the current prod replication-by-fp-id gaps/deletes problem). SQL-inspectable
  for repair/backfill.
- Signedness: hashes and fp ids are u32, PG int4 is signed -> bigint for fp_id,
  bytea (raw LE u32) for hashes. We never query individual hashes in SQL.
- Commit-order visibility gotcha: sequences don't commit in order; a poller can skip
  an uncommitted lower id forever. Single writer per index, or advisory lock in the
  insert path.
- Optimistic concurrency: `WHERE max(id) = $expected` in the insert tx
  (maps to Oplog.WriteOptions.expected_last_version).
- Retention: drop partitions below min consumer position. Only needs to cover
  max replica lag + newest snapshot age (days, not history).

### Position tracking (already built, from the NATS version)

Apply batches with `version = max(changelog id)` — the external log id IS the index
version (ClusterMultiIndex already does this with NATS stream seq). Consequences:

- Position is embedded in oplog/segments/manifest, so it rides inside snapshots. No sidecar.
- Idempotency guard `id > last_applied` already exists; gaps are fine (shared sequence
  across indexes).
- Consumer loop = `SELECT ... WHERE index_name=$1 AND id>$2 ORDER BY id LIMIT n`
  + LISTEN/NOTIFY. Replaces the whole JetStream consumer machinery.

### Bootstrap / repair

- New or corrupted node: fetch `_snapshot` from any live replica, restore, read embedded
  version, consume from there. Idempotent apply makes handoff overlap safe.
- Verify: snapshot must be a consistent cut (taken under the same locks as checkpointing).
- Node behind truncated log (position < oldest retained id): rebuild from snapshot.
- PG lost acked tail (async failover worst case): detect `position > max(id)`, refuse
  to serve, rebuild. Prevented almost entirely by synchronous replication.
- Nodes at same position are logically identical, NOT byte-identical (merge timing is
  local). Intentional: logical replication, every node runs the full write path, any
  node can be a snapshot donor. Don't do rsync/byte-level replication — it fights this.

## Storage: fully resident, no mmap

### Decision

Load all segments fully into anonymous memory at startup (io_uring reads), mlock, no mmap.
No buffer pool / explicit block cache — that's rebuilding the page cache to solve a
data >> RAM problem we've defined away.

Why no mmap: page faults are invisible to a green-thread scheduler (don't go through
any yield point), so a major fault stalls a carrier thread + everything queued on it.
Plus: SIGBUS error handling, no control over eviction, no verify-on-load.

### Details

- **Anonymous memory is the swappable kind** (file-backed clean pages just get dropped;
  anonymous pages go to swap). So mlock is required (mind RLIMIT_MEMLOCK / CAP_IPC_LOCK),
  or swapless hosts.
- **madvise(MADV_HUGEPAGE) BEFORE reading into the region** — write faults then populate
  2MB pages directly instead of waiting for khugepaged collapse. Big TLB win for
  random access over ~40GB.
- **mlock per segment region**, not mlockall(MCL_FUTURE) (would lock heap/stacks and
  make every allocation a potential failure).
- **mlock after the reads populated the pages** (mlock on unpopulated region faults
  everything in synchronously = seconds of stall). Even then run it via spawnBlocking.
- **Verify CRC64 (footer) while loading**, chunked reads + incremental CRC, overlap
  with I/O. Corruption = clean startup error instead of garbage results / SIGBUS mid-query.
- One reusable "load segment file into locked memory, verify CRC" function; used by
  startup, post-merge read-back, snapshot restore.
- Startup cost: ~40GB sequential at NVMe speed ~ 10s. Fine.
- Merge transient: merged segment (~2GB max) + inputs until readers drain. Line item
  in the memory budget.
- If data ever outgrows RAM: that reopens the buffer-pool option (io_uring block reads).
  Explicitly out of scope now.

## File format v2 (O_DIRECT-friendly)

Note: O_DIRECT alignment constraints apply to read calls (offset/buffer/length), not
file layout — whole-file sequential loads need no format change. Format changes are
for cleanliness + future partial-read paths + in-memory alignment (in-file alignment
becomes in-memory alignment when loading whole file into an aligned region).

- Pad every section to 4096 (not 512): fixed 4K header, then metadata / docs / blocks /
  block index each 4K-aligned. 4K works for 512e+4Kn disks and page-aligned buffers.
  Query real requirement via statx STX_DIOALIGN if needed.
- Section offsets/sizes go in the fixed header (layout up front). Keep the footer for
  CRC only (writer knows it only at the end).
- **Codec block != disk block.** 512B codec block is the unit of decompression per
  lookup; growing it to 4K = 8x more decode per random probe for no I/O benefit
  (there is no I/O at query time). Keep small codec blocks packed in 4K-aligned regions;
  tune codec block size on search benchmarks only. Smaller might even be better.
- Writes: buffered + fdatasync + fadvise(DONTNEED) first. O_DIRECT writes only if merge
  writeback churn measurably jitters searches (fully-resident searches are immune to
  page cache pressure, so probably not).
- **No backwards compatibility, period** (decided): the rewrite does not read v1
  segments, v1 oplogs, or v1 snapshots — no dual-format loader, no converter tool.
  Migration = rebuild nodes from the PG changelog / source DB; that rebuild path IS
  the migration. Still stamp a format version in the v2 header (for v3, someday).
  Note: this overrides the "don't break backwards compatibility" rule in CLAUDE.md
  for the rewrite — update CLAUDE.md in the new tree.

## zio migration

zio = stackful coroutines, multi-thread executors, io_uring on Linux, fully async file
I/O with positional reads (File.read(buffer, offset)), zio.sync primitives, Group
structured concurrency, spawnBlocking thread pool, std.Io interface for libraries.

### Clean mappings

- Segment loader: coroutine per segment in a Group; replaces SegmentLoadContext +
  WaitGroup + semaphore in Index.zig. Group.cancel = clean shutdown during startup.
  zio.sync.Semaphore if concurrent-load cap still wanted.
- **All std.Thread locks must become zio.sync equivalents** (segments_lock RwLock etc.).
  A std mutex held across a suspend blocks the carrier thread — same invisible-stall
  class as mmap faults. Mechanical but must be exhaustive.
- Scheduler.zig + background tasks -> long-lived coroutines per index (consumer loop,
  checkpoint, merge triggers).
- Search Deadline -> zio cancellation (timeout -> error.Canceled at suspension points).
- Executors ~ cores (searches are CPU-bound); blocking pool absorbs merges + mlock;
  file I/O costs no threads.

### Two runtimes: search/API + background (decided)

Data-plane/control-plane split. zio supports it (Runtime.init enable_main_executor=false
is documented for background-thread runtimes).

- **Search/API runtime**: HTTP + searches, strictly read-only on index state.
  Executors ~ cores.
- **Background runtime**: PG consumer, WAL appends, checkpoints, merges, manifest swaps —
  all mutation. ~2 executors. Optionally nice these threads so the kernel favors search
  under contention. Merges can stay plain coroutines here — they can only starve other
  background tasks, which is fine (no spawnBlocking needed for merges anymore).
- Oversubscription is fine: kernel preempts fairly, background idles when no merge runs.
- **Cross-runtime boundary**: zio sync primitives are cross-runtime safe (tested
  pattern), so segments_lock can stay a shared zio RwLock — the split is mechanical.
  Optional later optimization: RCU-style atomic SharedPtr swap for the segment lists
  (lock-free read path, writers serialize on background runtime) to remove
  reader/writer contention from search hot path entirely. Update requests from HTTP
  (_update path) hand off to the writer via channel.
- Blocking pool shrinks to genuinely blocking oddities (mlock, non-Linux fsync paths).

### Gaps / decisions

1. **No O_DIRECT flag in zio FileOpenFlags** (os/fs.zig) — add `direct: bool`,
   Linux-only semantics is fine. Consider exposing STX_DIOALIGN via statx.
2. **Merge scheduling.** Superseded by the two-runtime split (merges live on the
   background runtime). Context kept for the record: merges do yield on buffered-write
   flushes, but between flushes it's pure CPU (inputs RAM-resident, writes are the only
   yield points) — multiple ms of uninterruptible CPU per slice vs sub-ms searches, and
   zio has no work stealing yet (runtime.zig:558), so on a shared runtime search p99
   would depend on merge write-buffer size. Single-runtime fallback: spawnBlocking.
3. **mlock via spawnBlocking** (see storage notes).
4. **Ecosystem**: need HTTP server + PG client on the runtime; std.Io interface is the
   lever (Zig 0.16 libs should work). Check LISTEN/NOTIFY support in whatever PG client;
   fallback is short-interval polling, acceptable at our write rates.

### Search path (unchanged semantics)

Search does zero I/O once resident. Per-query stack use (block cache = 4 BlockReaders
~= 64KB) is fine with zio growable stacks.

## The log is an interface (decided)

No standalone local WAL *alongside* the PG changelog — instead, one Log abstraction
with two implementations, chosen by deployment mode:

- **Cluster mode**: PG changelog impl. No node-local WAL at all. Recovery = load
  segments, read max checkpointed version, consume from PG. No per-batch fsync on the
  apply path (durability is PG's; replica acks mean nothing) -> replicas apply at RAM
  speed. Snapshots = manifest + immutable segments only. Changelog retention must cover
  checkpoint lag + worst node downtime. Accepted edge: crash + PG outage together ->
  node serves from last checkpoint until PG returns (mitigate: checkpoint on graceful
  shutdown + age-based checkpoint trigger to bound the replay window).
- **Standalone mode**: file-backed log impl (the oplog reborn, but as the
  *authoritative* log, not a shadow). Keeps fsync-per-append (durability is its job
  here), rotation, delete-after-checkpoint retention — all inside the impl.

Interface: append(ops) -> position (standalone/API writes), readFrom(position),
notify. Everything downstream is identical in both modes: consumer loop,
version-as-position, recovery path, snapshot semantics. Standalone = a one-node
cluster whose log lives in a file. Two impls, one pipeline.

Read-your-writes: _update handler appends, gets position, waits for local
applied-watermark >= position before acking (small future keyed on position). Same
primitive gives read-your-writes on the receiving node in cluster mode if API writes
ever INSERT into PG directly.

Testing bonus: the file-log impl exercises all shared machinery (consumer, apply,
checkpoint, recovery, snapshots) with zero PG infra — most e2e tests run standalone,
only the PG impl needs heavier setup.

Trap to avoid: no cluster features in the file impl (no replicating from a file log,
no multi-node file-log anything). The file log never grows network legs.

## Rewrite approach (Zig 0.16 + zio)

In-place upgrade rejected: pinned to Zig 0.14, so 0.15 Writergate + 0.16 changes touch
every I/O line anyway, while the architecture changes underneath. Rewrite, transplanting
proven code. **No backwards compat of any kind** — no v1 readers, no converters;
migration = rebuild from changelog/source DB.

Three buckets:
- **Port nearly verbatim** (pure logic, tested, no I/O/threads): segment.zig,
  block.zig, streamvbyte.zig (pure Zig, @Vector + SIMD intrinsics + inline asm), segment_merge_policy.zig (Lucene-derived,
  subtle — never rewrite from memory), scoring in common.zig, Metadata.zig.
  Unit tests come along.
- **Reimplement, keep the shape**: filefmt (v2), FileSegment (simpler: heap slice, no
  mmap), MemorySegment, segment lists / SharedPtr snapshots, manifest, Oplog (reborn
  as the file-backed Log impl — see "The log is an interface").
- **Drop**: server.zig/httpz, ClusterMultiIndex + NATS, Scheduler/WaitGroup/FileLock
  utils, zul.

Anchor: the pytest e2e suite speaks HTTP and is implementation-agnostic — executable
spec of the old system. Point it at the new binary from week one; green count =
progress bar. (API may still evolve — adapt tests deliberately, not accidentally.)

Milestones (walking skeleton, each step runnable):
1. Fresh branch; Zig 0.16 + zio skeleton builds, /_health. Delete src2/ (stale Sep 2025
   copy of src/) + root debris.
2. Port pure modules + unit tests (mechanical; de-risks codec on 0.16).
3. Skeleton: zio HTTP, one index, memory segments only, _update/_search, JSON+msgpack.
   First e2e green; proves two-runtime wiring.
4. Log interface + file-backed impl (standalone mode works end-to-end, durable,
   with read-your-writes watermark).
5. Format v2 writer + resident loader (CRC, hugepage, mlock) + checkpointing.
6. Merging on background runtime.
7. PG changelog consumer + position-as-version; adapt replication e2e from NATS tests.
8. Snapshots/bootstrap, metrics, ops polish.

Genuinely new code is small: PG consumer, resident loader, v2 reader/writer, HTTP layer,
runtime wiring. Everything else is transplant.

Discipline: no redesigning while transplanting — improvements go into these notes, not
into the port. Old src/ in git history is the answer key.

## Deliberately absent

No consensus in fpindex, no leader among nodes, no node-to-node replication protocol,
no broker, no exactly-once machinery (ordered log + idempotent apply), no byte-level
file sync, no buffer pool. Each is delegated to PG/Patroni or made unnecessary by
log-position-as-version + immutable segments + fits-in-RAM.
