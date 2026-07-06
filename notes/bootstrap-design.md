# Snapshot + peer-restore bootstrapping

Companion to `design-notes-2026-07.md`. Covers how a new or behind replica gets an
index without replaying the whole changelog.

## Problem

The PG changelog has **bounded retention** — old positions are dropped once every live
consumer has moved past them. So a replica that is new, or was down longer than
retention, cannot replay its index from the log: the positions it needs are gone. It
must instead **fetch a snapshot from a live peer**, restore it, and resume consuming the
short remaining tail from the coordinator.

## What a snapshot is

**File segments + the manifest. Nothing else** — no WAL, no in-memory segments.

- The manifest is a msgpack array of `SegmentInfo`; each segment carries its version, so
  `file_version` F = max segment version = the snapshot's **watermark**. It already rides
  inside the manifest — no sidecar position file.
- Restore = extract → `open()` (which CRC-verifies every segment) → the index is at
  version F → **resume the data consumer at `(name, generation, after = F)`**. The tail
  `(F, now]` (the donor's memory segments plus anything newer) is re-fetched from the
  coordinator. Idempotent apply (`id > last_applied`) makes the handoff overlap safe, so
  any node can donate at any instant.

Why not include the WAL / memory segments: capturing them consistently races the donor's
own checkpoint/truncation (a checkpoint between pinning F and copying the WAL moves ops
into a new segment *and* truncates them from the WAL → gap). Segments-only sidesteps it —
the pinned reader is already a consistent, immutable cut. The cost is that the coordinator
must retain `[F, now]`, which is what the age-based checkpoint keeps small.

A **standalone backup** (self-contained, no coordinator to replay the tail) would need the
WAL captured under the checkpoint lock — a separate, later variant. Out of scope here.

## Donor (serving a snapshot)

`GET /:index/_snapshot` streams a `std.tar` of the manifest + file segments, built from a
**pinned reader** (`acquireReader` → a refcounted `Segments` snapshot). Because file
segments are immutable and fully **resident in memory** (`FileSegment.data`), the tar is
assembled straight from those heap buffers — **no file I/O, no lock**; the reader's
refcount keeps the segments alive for the duration of the stream (guaranteed by the
`delete_on_destroy` retirement rule). The response is tagged with the generation so the
restorer can verify the lineage.

## Coordinator as the peer registry

The coordinator is the rendezvous point — no separate node registry, no LB, no config peer
list. It already sees which replicas are live from their long-poll read connections; each
replica additionally sends a **periodic status heartbeat**:

    POST /_status
    { replica_id, advertise_addr,
      indexes: [ { name, generation, applied, file_version }, … ] }

The coordinator keeps `replica_id → { advertise_addr, last_seen, per-lineage {applied,
file_version} }`, expiring entries on a `last_seen` timeout. That single table serves:

1. **Donor discovery** — pick a live replica whose `file_version ≥ P` for `(name, gen)`.
2. **The below-retention trigger** — when a replica's `read(after = V)` finds
   `V < oldest_retained`, the coordinator replies "below retention → bootstrap from
   `<addr>` at F." Trigger + donor in one round-trip.
3. **Retention** — truncate below `min(file_version)` across live replicas. Retention
   safety keys on **`file_version`, not `applied`** (a donor's snapshot resumes from
   file_version). This is why a slow-to-checkpoint replica would pin retention — the
   age-based checkpoint prevents that.

Chosen over a per-checkpoint callback (goes stale for a quiet index, gives no liveness on
its own) and over piggybacking on the consume path (keeps the hot, per-lineage read path
clean). Config gains `--advertise-addr host:port`.

## Trigger & flow

On startup / meta-reconcile, per active index in the meta feed:

- local lineage absent, or wrong generation → **bootstrap**
- present but the coordinator's read signals `V < oldest_retained` → **re-bootstrap**
- present and within retention → **normal resume**

A **fresh cluster** has `oldest_retained = 0`, so `V = 0 ≥ 0` → normal replay; bootstrap
only fires once the log has actually been truncated (an established cluster), where at
least one live replica holds the data. So there is no cold-start chicken-and-egg.

Restore path: fetch tar from the donor → extract to a temp dir → verify (CRC on open +
generation match) → atomically rename into `v<generation>` → open → resume at F.

Also: `position > max(id)` (PG lost an acked tail after async failover) → refuse to serve,
rebuild. Prevented almost entirely by synchronous replication.

## Milestones

1. **Age-based checkpoint — DONE.** `Index.checkpoint_age` forces a flush once the oldest
   uncheckpointed write is that old, even below the size threshold — bounds WAL growth and
   keeps `file_version` within retention. `--checkpoint-age-ms` (default 60000, 0 disables).
2. **Donor.** In-memory tar from a pinned reader; wire `GET /:index/_snapshot`
   (generation-tagged). Reference: old `acoustid/idx/src/snapshot.zig` (drop its WAL
   inclusion and blocking I/O; port to zio).
3. **Coordinator registry.** `POST /_status` heartbeat + live-replica table, donor
   selection, below-retention read response. Replica gains a status-reporter coroutine.
   Wire into the `MemoryCoordinator` stub + `coordinator_server` + `RemoteCoordinator`;
   real retention/selection policy lands in the eventual PG gateway.
4. **Restorer + orchestration.** Fetch → temp-extract → verify → atomic rename into
   `v<gen>` → open → resume at F; per-index bootstrap off the below-retention trigger in
   the consumer; plus the `position > max(id)` refuse-and-rebuild guard.
