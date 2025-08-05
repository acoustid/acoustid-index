import requests
import pytest
import subprocess
import time
from urllib.parse import urljoin


class ServerNotReady(Exception):
    pass


class ServerDied(Exception):
    pass


class ServerManager:

    def __init__(self, base_dir, port):
        self.data_dir = base_dir / 'data'
        self.log_file = base_dir / 'server.log'
        self.port = port
        self.process = None

    def start(self):
        command = [
            'zig-out/bin/fpindex',
            '--dir', str(self.data_dir),
            '--port', str(self.port),
            '--log-level', 'debug',
            '--parallel-loading-threshold', '2',
        ]
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=self.log_file.open('a'),
        )
        self.wait_for_ready()

    def stop(self, kill=False):
        if self.process is not None:
            if self.process.returncode is None:
                if kill:
                    self.process.kill()
                else:
                    self.process.terminate()
                try:
                    self.process.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()

    def restart(self, kill=True):
        self.stop(kill=kill)
        self.start()

    def print_error_log(self):
        for line in self.error_log():
            print(line)

    def wait_for_ready(self, index_name=None, timeout=10.0):
        deadline = time.time() + timeout
        while True:
            if index_name:
                url = f'http://localhost:{self.port}/{index_name}/_health'
            else:
                url = f'http://localhost:{self.port}/_health'
            try:
                with requests.get(url) as res:
                    res.raise_for_status()
                    return
            except Exception:
                if time.time() > deadline:
                    self.print_error_log()
                    raise ServerNotReady()
                try:
                    self.process.wait(timeout / 100.0)
                except subprocess.TimeoutExpired:
                    continue
                else:
                    self.print_error_log()
                    raise ServerDied()

    def error_log(self):
        for line in self.log_file.read_text().splitlines():
            yield line


@pytest.fixture(scope='session')
def server(tmp_path_factory):
    srv = ServerManager(base_dir=tmp_path_factory.mktemp('srv'), port=26081)
    srv.start()
    try:
        yield srv
    finally:
        srv.stop()
        srv.print_error_log()


index_no = 1


@pytest.fixture
def index_name(request):
    global index_no
    index_no += 1
    return f't{index_no:03d}'


class Client:
    def __init__(self, session, base_url):
        self.session = session
        self.base_url = base_url

    def head(self, url, **kwargs):
        kwargs.setdefault('timeout', 1)
        return self.session.head(urljoin(self.base_url, url), **kwargs)

    def get(self, url, **kwargs):
        kwargs.setdefault('timeout', 1)
        return self.session.get(urljoin(self.base_url, url), **kwargs)

    def put(self, url, **kwargs):
        kwargs.setdefault('timeout', 1)
        return self.session.put(urljoin(self.base_url, url), **kwargs)

    def post(self, url, **kwargs):
        kwargs.setdefault('timeout', 1)
        return self.session.post(urljoin(self.base_url, url), **kwargs)

    def delete(self, url, **kwargs):
        kwargs.setdefault('timeout', 1)
        return self.session.delete(urljoin(self.base_url, url), **kwargs)


@pytest.fixture
def session():
    with requests.Session() as session:
        yield session


@pytest.fixture
def client(session, server):
    return Client(session, f'http://localhost:{server.port}')


@pytest.fixture()
def create_index(client, index_name):
    req = client.put(f'/{index_name}')
    req.raise_for_status()


@pytest.fixture(autouse=True)
def delete_index(client, index_name):
    yield
    req = client.delete(f'/{index_name}')
    req.raise_for_status()
