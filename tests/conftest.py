import requests
import pytest
from urllib.parse import urljoin


@pytest.fixture
def index_name():
    return 'testidx'


class Client:
    def __init__(self, session, base_url):
        self.session = session
        self.base_url = base_url

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
def client(session):
    return Client(session, 'http://localhost:8080')


@pytest.fixture(autouse=True)
def delete_index(client, index_name):
    req = client.delete(f'/{index_name}')
    req.raise_for_status()


@pytest.fixture()
def create_index(client, index_name, delete_index):
    req = client.put(f'/{index_name}')
    req.raise_for_status()
