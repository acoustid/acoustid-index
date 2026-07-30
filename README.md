# fpindex

Inverted index for searching audio fingerprints (an ID plus a set of hashes). Search
finds fingerprint IDs whose hash set intersects the query. Runs as an HTTP service,
standalone or as a cluster of replicas following a shared changelog.

## Build

    zig build              # -> ./zig-out/bin/fpindex
    zig build run          # run it
    zig build unit-tests   # unit tests
    zig build e2e-tests    # integration tests (see Testing)

## Running

Standalone, with a durable local log:

    ./zig-out/bin/fpindex --dir /var/lib/fpindex --port 8080

Cluster: every node is a replica consuming an ordered changelog from a coordinator.
The coordinator is either the built-in one (`--coordinator`) or an external service
speaking the same protocol (acoustid-server serves it from PostgreSQL).

    # built-in coordinator
    ./zig-out/bin/fpindex --coordinator --port 9000

    # replicas
    ./zig-out/bin/fpindex --port 8080 --dir /var/lib/fpindex \
        --coordinator-url http://coordinator:9000 \
        --peers http://fpindex-headless:8080

### Docker

`ghcr.io/acoustid/acoustid-index:main` is built from `main` on every push. It
serves on 8080 and keeps data in the `/var/lib/fpindex` volume, as user 6081.

    docker run --security-opt seccomp=unconfined \
        -p 8080:8080 -v fpindex-data:/var/lib/fpindex \
        ghcr.io/acoustid/acoustid-index:main

**`--security-opt seccomp=unconfined` is required.** Docker's default seccomp
profile blocks `io_uring_setup`, which the async runtime needs; without it the
server exits immediately with `error: PermissionDenied`. A custom profile that
allows the `io_uring_*` syscalls works too. Kubernetes only hits this if the pod
sets `seccompProfile: RuntimeDefault`.

The image is built from a binary produced outside it (`zig build --release=fast`),
so `docker build .` on a clean checkout fails until that has run.

## Configuration

| Flag | Default | Meaning |
| --- | --- | --- |
| `--dir PATH` | `data` | Data directory. |
| `--host HOST` | `127.0.0.1` | Listen address. |
| `--port PORT` | `8080` | Listen port. |
| `--checkpoint-threshold N` | `100000` | Flush memory segments to a file segment once they exceed this size. |
| `--checkpoint-age-ms MS` | `60000` | Also flush once the oldest unflushed write is this old. `0` disables. |
| `--legacy-port PORT` | `0` (off) | Listener for the legacy line protocol. |
| `--load-concurrency N` | `0` (unlimited) | Max file-segment loads in flight during startup. |
| `--coordinator` | off | Run as the changelog coordinator instead of an index node. |
| `--coordinator-url URL` | – | Replica mode: consume the changelog from this coordinator. |
| `--peers URLS` | – | Comma-separated base URLs to search for snapshot donors. Hostnames are re-resolved on every lookup, so one URL naming a Kubernetes headless Service covers the whole cluster. |
| `--bootstrap-timeout-ms MS` | `1800000` | Backstop on a whole bootstrap transfer (snapshot or corpus stream). `0` disables. Raise it if an initial fill legitimately streams longer. |

## HTTP API

Request and response bodies are JSON or MessagePack, negotiated via `Content-Type`
and `Accept`.

| Endpoint | Meaning |
| --- | --- |
| `GET /_health` | Process liveness. Always `200 OK` while the process runs. |
| `GET /_metrics` | Prometheus metrics. |
| `GET /:index/_health` | Index readiness: `200 OK` when serving, `503 LOADING` while the index is being filled by a bootstrap, `404` if it does not exist. |
| `PUT /:index` | Create an index. Body: `{"expect_does_not_exist": bool, "generation": u64}` (both optional). Idempotent. |
| `DELETE /:index` | Delete an index. Body: `{"expect_exists": bool}` (optional). |
| `GET /:index` | Index info: version, metadata, stats. |
| `POST /:index/_search` | Body: `{"query": [u32], "limit": u32, "timeout": u32, "min_score": u32, "score_pct": u32}` (all but `query` optional). Returns `{"results": [{"id", "score"}]}`. Answers `503` while the index is being filled by a bootstrap. |
| `POST /:index/_update` | Batch write. Body: `{"changes": [{"insert": {"id": u32, "hashes": [u32]}} \| {"delete": {"id": u32}}], "metadata": {..}, "expected_version": u64}`. Returns the new version. |
| `PUT /:index/:id` | Insert one fingerprint. Body: `{"hashes": [u32]}`. |
| `DELETE /:index/:id` | Delete one fingerprint. |
| `GET /:index/:id` | Fingerprint info (404 if absent). |
| `GET /:index/_status` | Replication watermarks: `{"generation", "version", "file_version"}`. `version` is the last applied changelog position; `file_version` is the highest position durable in file segments (what a snapshot from this node covers). |
| `GET /:index/_snapshot` | Stream a snapshot (manifest + file segments) of this node's index. |

Fingerprint IDs are unsigned 32-bit and must be nonzero.

### Health probes

Use `GET /_health` for liveness and peer discovery, and `GET /:index/_health` for
routing search traffic. A node whose index is still being filled by a bootstrap
answers `503 LOADING` on the index health and `503` on searches; it must stay
reachable by peers (liveness green) during that time.

## Storage

An index is fully RAM-resident: immutable file segments are loaded into locked
anonymous memory at startup and searched without I/O. Writes build in-memory
segments and are made durable by the local write-ahead log (standalone) or by the
upstream changelog (cluster). Checkpoints flush memory segments to file segments;
background merges compact file segments. The index must fit in RAM.

On disk, each index lives in `DIR/<name>/v<generation>/` with `data/` (manifest +
segments) and `oplog/` (write-ahead log) inside. `<name>/current` records the
active generation.

## Replication

Every index name is replicated as a *lineage* `(name, generation)`. A create mints
a new generation; delete + recreate always bumps it, so lineages never mix. The
changelog position of an applied entry is the index `version`, embedded in segments
and snapshots.

Two feeds, served by the coordinator:

- the **meta feed** (`GET /_meta`) lists index create/delete operations; each node
  reconciles its local set of indexes against it,
- the **data feed** (`GET /_changelog/:index/:gen?after=N`) serves a lineage's
  entries above a position. The server answers immediately and paces the client
  with `retry_after_ms`.

Writes on a replica (`_update`, `PUT /:index/:id`) are appended to the changelog
via the coordinator and acknowledged once the local consumer has applied them
(read-your-writes).

### Bootstrap

The changelog has bounded retention. A consumer asking for a position that has been
dropped gets `410`, and the node restores itself from a peer: it probes every
`--peers` URL, picks the donor with the highest `file_version` it can still resume
from, streams its snapshot, swaps it in, and resumes the feed at the snapshot's
watermark.

A node whose index is *empty* seeds itself before its first read, in this order:

1. a peer snapshot, if any donor has one,
2. the feed's corpus stream (`GET /_bootstrap/:index/:gen`), if the feed offers
   one — the stream is applied into a staging directory, flushed fully, and
   installed atomically; the node then resumes the feed at the stream's position
   and can immediately donate snapshots to other nodes,
3. plain replay from position 0.

While either bootstrap is filling an index, `GET /:index/_health` answers
`503 LOADING` and searches are refused with `503`.

On an initial fill of a new cluster, start one node first and let it finish
seeding; the remaining nodes then bootstrap from it as peers.

### Coordinator protocol

Spoken between replicas and the coordinator, msgpack-encoded:

| Endpoint | Meaning |
| --- | --- |
| `POST /_changelog/:index/:gen` | Append a batch of changes; supports optimistic `expected` version. |
| `GET /_changelog/:index/:gen?after=N&max=M` | Read entries above a position. `410` below the retention floor. |
| `PUT /_index/:name`, `DELETE /_index/:name` | Create / delete an index in the registry. |
| `GET /_meta?after=N&max=M` | Read index lifecycle operations. |
| `GET /_bootstrap/:index/:gen` | Stream the whole current corpus: a msgpack header `{position}`, then arrays of changes, terminated by an empty array. |
| `POST /_truncate/:index/:gen?floor=N` | Drop entries at or below a position (retention). |

## Legacy protocol

`--legacy-port` enables the line-based protocol of the previous fpindex version
(`search`, `begin`/`insert`/`commit`) against the `main` index.

## Testing

    zig build unit-tests
    TEST_FILTER=bootstrap zig build unit-tests   # substring filter, | separates alternatives
    TEST_VERBOSE=naming zig build unit-tests     # print each test as it runs

The integration suite drives the real binary over HTTP:

    python3 -m venv tests/venv
    tests/venv/bin/pip install -r tests/requirements.txt
    source tests/venv/bin/activate
    zig build e2e-tests    # or: pytest -v tests/
