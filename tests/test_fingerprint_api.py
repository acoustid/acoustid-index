import json


def test_insert(client, index_name, create_index):
    # insert fingerprint
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [100, 200, 300]}}
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    # verify we can find it
    req = client.post(f'/{index_name}/_search', json={
        'query': [100, 200, 300]
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': [{'id': 1, 'score': 3}]
    }


def test_update_full(client, index_name, create_index):
    # insert fingerprint
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [100, 200, 300]}}
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    # update fingerprint
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [1000, 2000, 3000]}}
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    # verify we can't find the original version
    req = client.post(f'/{index_name}/_search', json={
        'query': [100, 200, 300]
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': []
    }

    # verify we can't find the updated version
    req = client.post(f'/{index_name}/_search', json={
        'query': [1000, 2000, 3000]
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': [{'id': 1, 'score': 3}]
    }


def test_update_partial(client, index_name, create_index):
    # insert fingerprint
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [100, 200, 300]}}
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    # update fingerprint
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [100, 200, 999]}}
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    # verify we can't find the original version
    req = client.post(f'/{index_name}/_search', json={
        'query': [100, 200, 300]
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': [{'id': 1, 'score': 2}]
    }

    # verify we can't find the updated version
    req = client.post(f'/{index_name}/_search', json={
        'query': [100, 200, 999]
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': [{'id': 1, 'score': 3}]
    }


def test_delete(client, index_name, create_index):
    # insert fingerprint
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [100, 200, 300]}}
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    # delete fingerprint
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'delete': {'id': 1}}
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    # verify we can't find it
    req = client.post(f'/{index_name}/_search', json={
        'query': [100, 200, 300]
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': []
    }
