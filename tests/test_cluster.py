"""Multi-node replication: one coordinator + N replicas, no PostgreSQL.

Self-contained (manages its own processes) rather than using the single-process
`server` fixture, since it needs a coordinator and two replicas.
"""

import json
import os
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BINARY = os.path.join(REPO_ROOT, "zig-out", "bin", "fpindex")


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait(port, path, codes=(200, 404)):
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            r = urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=1)
            if r.status in codes:
                return
        except urllib.error.HTTPError as e:
            if e.code in codes:
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"port {port}{path} not ready")


def _req(port, method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    return json.loads(urllib.request.urlopen(r, timeout=10).read() or b"{}")


def _search(port, query):
    try:
        return _req(port, "POST", "/main/_search", {"query": query}).get("results", [])
    except urllib.error.HTTPError as e:
        if e.code == 503:  # still bootstrapping: refused, not empty — poll on
            return []
        raise


def _search_has(port, query, want_id, tries=50):
    for _ in range(tries):
        if any(h["id"] == want_id for h in _search(port, query)):
            return True
        time.sleep(0.1)
    return False


def _index_exists(port, name="main"):
    try:
        r = urllib.request.urlopen(f"http://127.0.0.1:{port}/{name}", timeout=2)
        return r.status == 200
    except urllib.error.HTTPError as e:
        return e.code == 200
    except Exception:
        return False


def _wait_index(port, name="main", want=True, tries=50):
    for _ in range(tries):
        if _index_exists(port, name) == want:
            return True
        time.sleep(0.1)
    return False


@pytest.fixture
def cluster(tmp_path):
    """Coordinator + two replicas. Yields (co_port, r1_port, r2_port)."""
    if not os.path.exists(BINARY):
        subprocess.run(["zig", "build"], cwd=REPO_ROOT, check=True)

    co, p1, p2 = _free_port(), _free_port(), _free_port()
    procs = []

    def start(args):
        procs.append(subprocess.Popen([BINARY] + args))

    start(["--coordinator", "--port", str(co)])
    _wait(co, "/_changelog/x/1?after=0&max=1&timeout_ms=50")
    url = f"http://127.0.0.1:{co}"
    start(["--port", str(p1), "--dir", str(tmp_path / "r1"), "--coordinator-url", url])
    start(["--port", str(p2), "--dir", str(tmp_path / "r2"), "--coordinator-url", url])
    _wait(p1, "/_health")
    _wait(p2, "/_health")
    try:
        yield co, p1, p2
    finally:
        for p in procs:
            p.send_signal(signal.SIGKILL)
        for p in procs:
            p.wait()


def test_index_create_propagates(cluster):
    _co, p1, p2 = cluster
    # Create on ONE replica; it must appear on the other via the meta feed.
    _req(p1, "PUT", "/main")
    assert _index_exists(p1, "main")  # create-your-writes on the writer
    assert _wait_index(p2, "main")    # propagates to the other replica

    # Writes then flow both ways on the shared index.
    _req(p1, "PUT", "/main/1", {"hashes": [100, 200, 300, 55555]})
    assert _search_has(p1, [100, 200, 300], 1)
    assert _search_has(p2, [100, 200, 300], 1)

    _req(p2, "PUT", "/main/2", {"hashes": [400, 500, 600, 55555]})
    assert _search_has(p2, [400, 500, 600], 2)
    assert _search_has(p1, [400, 500, 600], 2)

    # Both replicas converge on both docs (shared hash).
    assert {h["id"] for h in _search(p1, [55555])} == {1, 2}
    assert {h["id"] for h in _search(p2, [55555])} == {1, 2}


def test_metadata_replicates(cluster):
    _co, p1, p2 = cluster
    _req(p1, "PUT", "/main")
    assert _wait_index(p2, "main")

    # Metadata-only update on p1 rides the op stream and replicates to p2.
    _req(p1, "POST", "/main/_update", {"changes": [], "metadata": {"foo": "bar", "rev": "7"}})

    def meta(port):
        return _req(port, "GET", "/main").get("metadata", {})

    assert meta(p1).get("foo") == "bar"  # read-your-writes on the writer
    for _ in range(50):
        if meta(p2).get("foo") == "bar" and meta(p2).get("rev") == "7":
            break
        time.sleep(0.1)
    assert meta(p2) == {"foo": "bar", "rev": "7"}


def test_index_delete_and_recreate_converges(cluster):
    _co, p1, p2 = cluster
    _req(p1, "PUT", "/main")
    assert _wait_index(p2, "main")
    _req(p1, "PUT", "/main/1", {"hashes": [1, 2, 3]})
    assert _search_has(p2, [1, 2, 3], 1)

    # Delete on p1 -> both converge to "gone".
    _req(p1, "DELETE", "/main")
    assert _wait_index(p1, "main", want=False)
    assert _wait_index(p2, "main", want=False)

    # Recreate on p2 -> new lineage on both; the old lineage's doc stays gone
    # (isolation is the generation scope), and new writes propagate.
    _req(p2, "PUT", "/main")
    assert _wait_index(p1, "main")
    assert not _search_has(p1, [1, 2, 3], 1, tries=5)
    _req(p2, "PUT", "/main/9", {"hashes": [7, 8, 9]})
    assert _search_has(p1, [7, 8, 9], 9)


def test_peer_status_endpoint(cluster):
    """GET /:index/_status is the whole peer-facing protocol: what a probing node
    reads to decide whether this one can donate a snapshot."""
    _co, p1, _p2 = cluster

    # Unknown index -> 404, so a peer that doesn't hold it simply isn't a donor.
    with pytest.raises(urllib.error.HTTPError) as exc:
        _req(p1, "GET", "/nope/_status")
    assert exc.value.code == 404

    _req(p1, "PUT", "/main")
    assert _wait_index(p1, "main")
    _req(p1, "PUT", "/main/1", {"hashes": [10, 20, 30]})
    assert _search_has(p1, [10, 20, 30], 1)

    st = _req(p1, "GET", "/main/_status")
    assert st["generation"] >= 1
    assert st["version"] >= 1
    # Nothing checkpointed yet, so a snapshot from here resumes at 0. Donor selection
    # keys on file_version rather than version for exactly this reason.
    assert st["file_version"] == 0


def test_new_node_bootstraps_from_peer(tmp_path):
    """A new node joins after the changelog is truncated past position 0: it can't
    replay from scratch, so it finds a donor among its configured peers, fetches a
    snapshot and resumes from its watermark."""
    if not os.path.exists(BINARY):
        subprocess.run(["zig", "build"], cwd=REPO_ROOT, check=True)

    co, p1, p2 = _free_port(), _free_port(), _free_port()
    procs = []

    def start(args):
        procs.append(subprocess.Popen([BINARY] + args))

    def file_version(port):
        try:
            return _req(port, "GET", "/main/_status")["file_version"]
        except Exception:
            return 0

    try:
        start(["--coordinator", "--port", str(co)])
        _wait(co, "/_changelog/x/1?after=0&max=1&timeout_ms=50")
        url = f"http://127.0.0.1:{co}"

        # r1 checkpoints each update (threshold 1) so it holds donatable file segments.
        start(["--port", str(p1), "--dir", str(tmp_path / "r1"), "--coordinator-url", url,
               "--checkpoint-threshold", "1"])
        _wait(p1, "/_health")
        _req(p1, "PUT", "/main")
        _req(p1, "PUT", "/main/1", {"hashes": [10, 20, 30]})
        _req(p1, "PUT", "/main/2", {"hashes": [40, 50, 60]})
        assert _search_has(p1, [10, 20, 30], 1)

        # Wait until r1 has checkpointed, i.e. holds a segment worth donating.
        deadline = time.time() + 10
        while time.time() < deadline and file_version(p1) < 1:
            time.sleep(0.1)
        assert file_version(p1) >= 1, "r1 never checkpointed a donatable segment"

        # Truncate the changelog below r1's watermark: a fresh consumer at 0 must bootstrap.
        req = urllib.request.Request(f"http://127.0.0.1:{co}/_truncate/main/1?floor=1", method="POST")
        assert urllib.request.urlopen(req, timeout=5).status == 200

        # New node joins: it can't replay from 0 (truncated), so it probes its peer
        # list. r2 is in its own list on purpose — it must not pick itself, though
        # note the unit tests in src/peers.zig are what actually pin that rule down
        # (here r1 would win on freshness regardless).
        peers = f"http://127.0.0.1:{p1},http://127.0.0.1:{p2}"
        start(["--port", str(p2), "--dir", str(tmp_path / "r2"), "--coordinator-url", url,
               "--peers", peers])
        _wait(p2, "/_health")
        assert _wait_index(p2, "main")
        assert _search_has(p2, [10, 20, 30], 1)
        assert _search_has(p2, [40, 50, 60], 2)
    finally:
        for p in procs:
            p.send_signal(signal.SIGKILL)
        for p in procs:
            p.wait()


def test_new_node_bootstraps_from_the_feeds_source_stream(tmp_path):
    """A new node with NO peers joins after the changelog is truncated past 0: the
    feed's own bootstrap stream (GET /_bootstrap — the same protocol acoustid-server
    serves from PostgreSQL) fills it, and the feed resumes above the stream's
    position."""
    if not os.path.exists(BINARY):
        subprocess.run(["zig", "build"], cwd=REPO_ROOT, check=True)

    co, p1, p2 = _free_port(), _free_port(), _free_port()
    procs = []

    def start(args):
        procs.append(subprocess.Popen([BINARY] + args))

    try:
        start(["--coordinator", "--port", str(co)])
        _wait(co, "/_changelog/x/1?after=0&max=1&timeout_ms=50")
        url = f"http://127.0.0.1:{co}"

        start(["--port", str(p1), "--dir", str(tmp_path / "r1"), "--coordinator-url", url])
        _wait(p1, "/_health")
        _req(p1, "PUT", "/main")
        _req(p1, "PUT", "/main/1", {"hashes": [10, 20, 30]})
        _req(p1, "PUT", "/main/2", {"hashes": [40, 50, 60]})
        assert _search_has(p1, [10, 20, 30], 1)

        # Truncate below the corpus: a replay from 0 is now impossible, and with no
        # peers configured the source stream is the only way in.
        req = urllib.request.Request(f"http://127.0.0.1:{co}/_truncate/main/1?floor=1", method="POST")
        assert urllib.request.urlopen(req, timeout=5).status == 200

        start(["--port", str(p2), "--dir", str(tmp_path / "r2"), "--coordinator-url", url])
        _wait(p2, "/_health")
        assert _wait_index(p2, "main")
        assert _search_has(p2, [10, 20, 30], 1)
        assert _search_has(p2, [40, 50, 60], 2)

        # And the feed resumed above the stream's position: a fresh write reaches
        # the bootstrapped node through the normal consumer path.
        _req(p1, "PUT", "/main/3", {"hashes": [70, 80, 90]})
        assert _search_has(p2, [70, 80, 90], 3)
    finally:
        for p in procs:
            p.send_signal(signal.SIGKILL)
        for p in procs:
            p.wait()
