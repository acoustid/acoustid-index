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

## Peer discovery: a list, not a registry

> Superseded 2026-07-29. This section previously put a replica registry on the
> coordinator (`POST /_status` heartbeats, a `last_seen` table, `GET /_donor`). That is
> removed — see below for why and for what replaced it.

Nodes find donors among themselves. Config is a list of peer base URLs
(`--peers http://a:8080,http://b:8080`), and **hostnames are re-resolved on every donor
lookup** — so one URL naming a Kubernetes headless Service covers the whole cluster and
follows it as pods come and go. Two endpoints, both on the node's existing server:

    GET /:index/_status     -> { generation, version, file_version }
    GET /:index/_snapshot   -> tar (already there)

A bootstrapping node probes every peer concurrently and takes the highest `file_version`
that is **strictly greater than the position it is stuck at**. That single condition does
two jobs: it guarantees forward progress (a snapshot at or below where we already are
would re-trigger the same below-retention read), and it excludes the node from its own
peer list for free — so no node identity, and no `--advertise-addr`, is needed anywhere.

Why not the registry: it made the log implementation **stateful, and therefore a
singleton**. Two coordinator/gateway instances behind a Service each see a different
subset of the heartbeats, so they disagree about who can donate and — worse — each
computes retention from a partial view. Keeping it would have meant putting the registry
in PG: a heartbeat write per replica every few seconds, on the primary, purely for
liveness. The log half is happily stateless; don't weld it to something that isn't.

What this costs: donor selection no longer rides along on the below-retention read, so
bootstrap takes one extra round trip on a path that runs approximately never. And
retention can no longer key on `min(file_version)` across live replicas, because the log
no longer knows the replicas — it becomes time-based. That is safe *because* bootstrap
works: falling off the log stops being fatal and becomes recoverable, which demotes
retention from a correctness mechanism to a tuning knob. Keep the window well past
`--checkpoint-age-ms` (a live peer's `file_version` is at most that stale), and alert on
`oldest_retained` vs each node's `file_version` rather than trying to enforce it.

**Kubernetes trap:** a headless Service publishes only *Ready* endpoints. Node readiness
must therefore mean "index open and serving", **not** "caught up with the log" — else a
cold cluster restart deadlocks: nobody is ready, DNS is empty, nobody can find a donor,
nobody becomes ready. Filter unsuitable donors on what `_status` reports, never via DNS.

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
3. **Peer discovery — DONE (2026-07-29), replaces the old "coordinator registry".**
   `peers.zig` (URL list, DNS re-resolved per lookup, concurrent probes, donor choice)
   + `GET /:index/_status` + `--peers`. The registry, heartbeats and `GET /_donor` are
   gone from `Coordinator`/`coordinator_server`/`RemoteCoordinator`. Each probe is
   bounded by a `zio.AutoCancel` (`probe_timeout`, default 5s), so a peer that accepts
   and then wedges costs the fan-out one timeout instead of hanging it.
4. **Restorer + orchestration.** Fetch → temp-extract → verify → atomic rename into
   `v<gen>` → open → resume at F; per-index bootstrap off the below-retention trigger in
   the consumer; plus the `position > max(id)` refuse-and-rebuild guard.
5. **Time-based retention** in the log implementation, now that it no longer has replica
   positions to key on. Export `oldest_retained`.
