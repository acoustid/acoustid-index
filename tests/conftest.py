import requests
import pytest
import subprocess
import time
import socket
from urllib.parse import urljoin


class ServerManager:

    def __init__(self, base_dir, port):
        self.data_dir = base_dir / 'data'
        self.log_file = base_dir / 'server.log'
        self.port = port
        self.process = None

    def start(self):
        command = [
            'valgrind',
            'zig-out/bin/fpindex',
            '--dir', str(self.data_dir),
            '--port', str(self.port),
            '--log-level', 'debug',
        ]
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=self.log_file.open('w'),
        )

    def stop(self):
        if self.process is not None:
            if self.process.returncode is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()

    def error_log(self):
        for line in self.log_file.read_text().splitlines():
            yield line


@pytest.fixture(scope='session')
def server(tmp_path_factory):
    srv = ServerManager(base_dir=tmp_path_factory.mktemp('srv'), port=14502)
    srv.start()
    yield srv
    srv.stop()
    for line in srv.error_log():
        print(line)


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


def check_port(port_no):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(('127.0.0.1', port_no)) == 0


def wait_for_ready(port, timeout):
    deadline = time.time() + timeout
    while not check_port(port):
        if time.time() > deadline:
            raise TimeoutError()
        time.sleep(timeout / 100.0)


@pytest.fixture
def client(session, server):
    wait_for_ready(server.port, 1)
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
