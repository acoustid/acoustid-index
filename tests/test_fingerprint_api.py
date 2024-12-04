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

    # verify we can get id
    req = client.get(f'/{index_name}/1')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'version': 1,
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

    # verify we can get id
    req = client.get(f'/{index_name}/1')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'version': 2,
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

    # verify we can get id
    req = client.get(f'/{index_name}/1')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'version': 2,
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

    # verify we can get id
    req = client.get(f'/{index_name}/1')
    assert req.status_code == 404, req.content
    assert json.loads(req.content) == {
        'error': 'FingerprintNotFound',
    }


def test_persistence_after_soft_restart(server, client, index_name, create_index):
    # insert fingerprint
    for i in range(100):
        body = {
            'changes': [
                {'insert': {'id': 1, 'hashes': [100+i, 200+i, 300+i]}}
            ],
        }
        with client.post(f'/{index_name}/_update', json=body) as req:
            assert req.status_code == 200, req.content
            assert json.loads(req.content) == {}

    server.restart()
    server.wait_for_ready(index_name, timeout=10.0)

    # verify we can find it
    req = client.post(f'/{index_name}/_search', json={
        'query': [199, 299, 399]
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': [{'id': 1, 'score': 3}]
    }

    # verify we can get id
    req = client.get(f'/{index_name}/1')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'version': 100,
    }


def test_persistence_after_hard_restart(server, client, index_name, create_index):
    # insert fingerprint
    for i in range(100):
        body = {
            'changes': [
                {'insert': {'id': 1, 'hashes': [100+i, 200+i, 300+i]}}
            ],
        }
        with client.post(f'/{index_name}/_update', json=body) as req:
            assert req.status_code == 200, req.content
            assert json.loads(req.content) == {}
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    server.restart(kill=True)
    server.wait_for_ready(index_name, timeout=10.0)

    # verify we can find it
    req = client.post(f'/{index_name}/_search', json={
        'query': [199, 299, 399]
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': [{'id': 1, 'score': 3}]
    }

    # verify we can get id
    req = client.get(f'/{index_name}/1')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'version': 100,
    }
