import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from urllib.parse import urljoin

import pytest
import requests


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BINARY = os.path.join(REPO_ROOT, "zig-out", "bin", "fpindex")


def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    """Runs the fpindex binary as a subprocess against a persistent data dir, so
    restart tests keep their data. One instance per test session."""

    def __init__(self):
        self.data_dir = tempfile.mkdtemp(prefix="fpindex-e2e-")
        self.port = _free_port()
        self.proc = None

    def get_url(self):
        return f"http://127.0.0.1:{self.port}"

    def start(self):
        assert self.proc is None
        self.proc = subprocess.Popen(
            [BINARY, "--dir", self.data_dir, "--host", "127.0.0.1", "--port", str(self.port)],
        )
        self.wait_for_healthy()

    def stop(self, kill=False):
        if self.proc is None:
            return
        self.proc.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.proc = None

    def restart(self, kill=False):
        self.stop(kill=kill)
        self.start()

    def wait_for_healthy(self, timeout=30):
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                raise RuntimeError(f"server exited early with code {self.proc.returncode}")
            try:
                r = requests.get(urljoin(self.get_url(), "/_health"), timeout=1)
                if r.status_code == 200:
                    return
            except requests.RequestException as e:
                last_err = e
            time.sleep(0.1)
        raise RuntimeError(f"server not healthy after {timeout}s: {last_err}")

    def cleanup(self):
        self.stop()
        shutil.rmtree(self.data_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def _built():
    """Build the binary once before the suite runs."""
    subprocess.run(["zig", "build"], cwd=REPO_ROOT, check=True)
    assert os.path.exists(BINARY), f"binary not found at {BINARY}"


@pytest.fixture(scope="session")
def server(_built):
    srv = Server()
    srv.start()
    try:
        yield srv
    finally:
        srv.cleanup()


index_no = 1


@pytest.fixture
def index_name(request):
    global index_no
    index_no += 1
    return f"t{index_no:03d}"


class Client:
    def __init__(self, session, base_url):
        self.session = session
        self.base_url = base_url

    def head(self, url, **kwargs):
        kwargs.setdefault("timeout", 30)
        return self.session.head(urljoin(self.base_url, url), **kwargs)

    def get(self, url, **kwargs):
        kwargs.setdefault("timeout", 30)
        return self.session.get(urljoin(self.base_url, url), **kwargs)

    def put(self, url, **kwargs):
        kwargs.setdefault("timeout", 30)
        return self.session.put(urljoin(self.base_url, url), **kwargs)

    def post(self, url, **kwargs):
        kwargs.setdefault("timeout", 30)
        return self.session.post(urljoin(self.base_url, url), **kwargs)

    def delete(self, url, **kwargs):
        kwargs.setdefault("timeout", 30)
        return self.session.delete(urljoin(self.base_url, url), **kwargs)


@pytest.fixture
def session():
    with requests.Session() as session:
        yield session


@pytest.fixture
def client(server, session):
    return Client(session, server.get_url())


@pytest.fixture()
def create_index(client, index_name):
    req = client.put(f"/{index_name}")
    req.raise_for_status()


@pytest.fixture(autouse=True)
def delete_index(client, index_name):
    yield
    client.delete(f"/{index_name}")
