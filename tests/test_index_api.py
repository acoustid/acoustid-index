import json


def test_get_index_not_found(client, index_name):
    req = client.get(f'/{index_name}')
    assert req.status_code == 404, req.content
    assert json.loads(req.content) == {'error': 'IndexNotFound'}


def test_get_index(client, index_name, create_index):
    req = client.get(f'/{index_name}')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {'version': 0, 'attributes': []}


def test_create_index(client, index_name):
    req = client.get(f'/{index_name}')
    assert req.status_code == 404, req.content

    req = client.put(f'/{index_name}')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    req = client.put(f'/{index_name}')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    req = client.get(f'/{index_name}')
    assert req.status_code == 200, req.content


def test_delete_index(client, index_name, create_index):
    req = client.get(f'/{index_name}')
    assert req.status_code == 200, req.content

    req = client.delete(f'/{index_name}')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    req = client.delete(f'/{index_name}')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    req = client.get(f'/{index_name}')
    assert req.status_code == 404, req.content
