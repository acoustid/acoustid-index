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
    return _req(port, "POST", "/main/_search", {"query": query}).get("results", [])


def _search_has(port, query, want_id, tries=50):
    for _ in range(tries):
        if any(h["id"] == want_id for h in _search(port, query)):
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
    _wait(co, "/_changelog/x?after=0&max=1&timeout_ms=50")
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


def test_writes_propagate_both_ways(cluster):
    _co, p1, p2 = cluster
    # Index lifecycle is local for now, so create it on each replica.
    _req(p1, "PUT", "/main")
    _req(p2, "PUT", "/main")

    # Write on replica 1: read-your-writes locally, and it reaches replica 2.
    _req(p1, "PUT", "/main/1", {"hashes": [100, 200, 300, 55555]})
    assert _search_has(p1, [100, 200, 300], 1)
    assert _search_has(p2, [100, 200, 300], 1)

    # Write on replica 2: reaches replica 1 (multi-master).
    _req(p2, "PUT", "/main/2", {"hashes": [400, 500, 600, 55555]})
    assert _search_has(p2, [400, 500, 600], 2)
    assert _search_has(p1, [400, 500, 600], 2)

    # Both replicas converge on both docs (shared hash).
    assert {h["id"] for h in _search(p1, [55555])} == {1, 2}
    assert {h["id"] for h in _search(p2, [55555])} == {1, 2}
