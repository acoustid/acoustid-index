import requests
import pytest
import subprocess
import time
import socket
from urllib.parse import urljoin


@pytest.fixture()
def server_port():
    sock = socket.socket()
    sock.bind(('', 0))
    return sock.getsockname()[1]


@pytest.fixture()
def server(server_port, tmp_path):
    data_dir = tmp_path / 'data'
    stderr = tmp_path / 'server.stderr.log'
    command = [
        'zig-out/bin/fpindex',
        '--dir', str(data_dir),
        '--port', str(server_port),
        '--log-level', 'debug',
    ]
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=stderr.open('w'),
    )
    yield
    if process.returncode is None:
        process.terminate()
        process.wait()
    for line in stderr.read_text().splitlines():
        print(line)


@pytest.fixture
def index_name():
    return 'testidx'


class Client:
    def __init__(self, session, base_url):
        self.session = session
        self.base_url = base_url

    def head(self, url, **kwargs):
        return self.session.head(urljoin(self.base_url, url), **kwargs)

    def get(self, url, **kwargs):
        return self.session.get(urljoin(self.base_url, url), **kwargs)

    def put(self, url, **kwargs):
        return self.session.put(urljoin(self.base_url, url), **kwargs)

    def post(self, url, **kwargs):
        return self.session.post(urljoin(self.base_url, url), **kwargs)

    def delete(self, url, **kwargs):
        return self.session.delete(urljoin(self.base_url, url), **kwargs)


@pytest.fixture
def session():
    with requests.Session() as session:
        yield session


def check_port(port_no):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(('127.0.0.1', port_no)) == 0


@pytest.fixture
def client(session, server_port, server):
    deadline = time.time() + 1
    while not check_port(server_port):
        if time.time() > deadline:
            raise TimeoutError()
        time.sleep(0.01)
    return Client(session, f'http://localhost:{server_port}')


@pytest.fixture()
def create_index(client, index_name):
    req = client.put(f'/{index_name}')
    req.raise_for_status()
