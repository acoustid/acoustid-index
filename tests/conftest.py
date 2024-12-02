import requests
import pytest
import subprocess
import socket
from urllib.parse import urljoin


@pytest.fixture(scope='session')
def server_port():
    sock = socket.socket()
    sock.bind(('', 0))
    return sock.getsockname()[1]


@pytest.fixture(scope='session')
def server(server_port, tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("server")
    data_dir = tmp_dir / 'data'
    stderr = tmp_dir / 'server.stderr.log'
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
    process.terminate()
    retcode = process.wait()
    if retcode != 0:
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


@pytest.fixture
def client(session, server_port, server):
    return Client(session, f'http://localhost:{server_port}')


@pytest.fixture(autouse=True)
def delete_index(client, index_name):
    req = client.delete(f'/{index_name}')
    req.raise_for_status()


@pytest.fixture()
def create_index(client, index_name, delete_index):
    req = client.put(f'/{index_name}')
    req.raise_for_status()
