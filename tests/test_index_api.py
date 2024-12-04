import pytest
import json
import msgpack


def test_head_index_not_found(client, index_name):
    req = client.head(f'/{index_name}')
    assert req.status_code == 404, req.content
    assert req.content == b''


def test_head_index(client, index_name, create_index):
    req = client.head(f'/{index_name}')
    assert req.status_code == 200, req.content
    assert req.content == b''


def test_get_index_not_found(client, index_name):
    req = client.get(f'/{index_name}')
    assert req.status_code == 404, req.content
    assert json.loads(req.content) == {'error': 'IndexNotFound'}


@pytest.mark.parametrize('fmt', ['json', 'msgpack'])
def test_get_index(client, index_name, create_index, fmt):
    req = client.get(f'/{index_name}', headers=headers(fmt))
    assert req.status_code == 200, req.content

    if fmt == 'json':
        expected = {'version': 0, 'attributes': []}
    else:
        expected = {'v': 0, 'a': {}}
    assert decode(fmt, req.content) == expected


def headers(fmt):
    if fmt == 'json':
        return {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
    elif fmt == 'msgpack':
        return {
            'Content-Type': 'application/vnd.msgpack',
            'Accept': 'application/vnd.msgpack',
        }
    else:
        return {}


def decode(fmt, content):
    if fmt == 'json':
        return json.loads(content)
    elif fmt == 'msgpack':
        return msgpack.loads(content)
    else:
        assert False


@pytest.mark.parametrize('fmt', ['json', 'msgpack'])
def test_create_index(client, index_name, fmt):
    req = client.head(f'/{index_name}')
    assert req.status_code == 404, req.content

    req = client.put(f'/{index_name}', headers=headers(fmt))
    assert req.status_code == 200, req.content
    assert decode(fmt, req.content) == {}

    req = client.put(f'/{index_name}', headers=headers(fmt))
    assert req.status_code == 200, req.content
    assert decode(fmt, req.content) == {}

    req = client.head(f'/{index_name}')
    assert req.status_code == 200, req.content


@pytest.mark.parametrize('fmt', ['json', 'msgpack'])
def test_delete_index(client, index_name, create_index, fmt):
    req = client.head(f'/{index_name}')
    assert req.status_code == 200, req.content

    req = client.delete(f'/{index_name}', headers=headers(fmt))
    assert req.status_code == 200, req.content
    assert decode(fmt, req.content) == {}

    req = client.delete(f'/{index_name}', headers=headers(fmt))
    assert req.status_code == 200, req.content
    assert decode(fmt, req.content) == {}

    req = client.head(f'/{index_name}')
    assert req.status_code == 404, req.content