import json
import random


def test_insert_single(client, index_name, create_index):
    # insert fingerprint
    req = client.put(f'/{index_name}/1', json={'hashes': [101, 201, 301]})
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    # verify we can find it
    req = client.post(f'/{index_name}/_search', json={
        'query': [101, 201, 301]
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': [
            {'id': 1, 'score': 3},
        ],
    }

    req = client.get(f'/{index_name}/1')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'version': 1,
    }


def test_insert_multi(client, index_name, create_index):
    # insert fingerprint
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [101, 201, 301]}},
            {'insert': {'id': 2, 'hashes': [102, 202, 302]}},
        ],
    })
    assert req.status_code == 200, req.content
    response = json.loads(req.content)
    assert 'version' in response
    assert response['version'] > 0

    # verify we can find it
    req = client.post(f'/{index_name}/_search', json={
        'query': [101, 201, 301, 102, 202, 302]
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': [
            {'id': 1, 'score': 3},
            {'id': 2, 'score': 3},
        ],
    }

    req = client.get(f'/{index_name}/1')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'version': 1,
    }

    req = client.get(f'/{index_name}/2')
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'version': 1,
    }


def test_insert_many(client, index_name, create_index):
    # insert fingerprints
    batch = []
    batch_size = 1000
    max_hash = 2**18
    for i in range(1, 50000 + 1):
        rng = random.Random(i)
        hashes = [rng.randint(0, max_hash) for _ in range(100)]
        batch.append({'insert': {'id': i, 'hashes': hashes}})
        if len(batch) == batch_size:
            req = client.post(f'/{index_name}/_update', json={
                'changes': batch,
            })
            assert req.status_code == 200, req.content
            batch = []
    if batch:
        ent.post(f'/{index_name}/_update', json={
            'changes': batch,
        })
        assert req.status_code == 200, req.content

    # verify we can find it
    rng = random.Random(100)
    hashes = [rng.randint(0, max_hash) for _ in range(100)]
    req = client.post(f'/{index_name}/_search', json={
        'query': hashes,
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': [
            {'id': 100, 'score': 100},
        ],
    }


def test_update_full(client, index_name, create_index):
    # insert fingerprint
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [100, 200, 300]}}
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content)['version'] > 0

    # update fingerprint
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [1000, 2000, 3000]}}
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content)['version'] > 0

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
    assert json.loads(req.content)['version'] > 0

    # update fingerprint
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [100, 200, 999]}}
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content)['version'] > 0

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


def test_delete_multi(client, index_name, create_index):
    # insert fingerprints
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [101, 201, 301]}},
            {'insert': {'id': 2, 'hashes': [102, 202, 302]}},
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content)['version'] > 0

    # delete fingerprints
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'delete': {'id': 1}},
            {'delete': {'id': 2}},
        ],
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content)['version'] > 0

    # verify we can't find it
    req = client.post(f'/{index_name}/_search', json={
        'query': [101, 201, 301, 102, 202, 302]
    })
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {
        'results': []
    }

    req = client.get(f'/{index_name}/1')
    assert req.status_code == 404, req.content
    assert json.loads(req.content) == {
        'error': 'FingerprintNotFound',
    }

    req = client.get(f'/{index_name}/2')
    assert req.status_code == 404, req.content
    assert json.loads(req.content) == {
        'error': 'FingerprintNotFound',
    }


def test_delete_single(client, index_name, create_index):
    # insert fingerprint
    req = client.put(f'/{index_name}/1', json={'hashes': [100, 200, 300]})
    assert req.status_code == 200, req.content
    assert json.loads(req.content) == {}

    # delete fingerprint
    req = client.delete(f'/{index_name}/1')
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
            assert json.loads(req.content)['version'] > 0

    server.restart()
    server.wait_for_healthy(timeout=10)

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
            assert json.loads(req.content)['version'] > 0
    assert req.status_code == 200, req.content
    assert json.loads(req.content)['version'] > 0

    server.restart(kill=True)
    server.wait_for_healthy(timeout=10)

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



def test_expected_version_validation(client, index_name, create_index):
    # First update to get initial version
    req = client.post(f"/{index_name}/_update", json={
        "changes": [{"insert": {"id": 1, "hashes": [100, 200, 300]}}]
    })
    assert req.status_code == 200, req.content
    version1 = json.loads(req.content)["version"]

    # Update with correct expected_version should succeed
    req = client.post(f"/{index_name}/_update", json={
        "changes": [{"insert": {"id": 2, "hashes": [101, 201, 301]}}],
        "expected_version": version1
    })
    assert req.status_code == 200, req.content

    # Update with wrong expected_version should fail with 409
    req = client.post(f"/{index_name}/_update", json={
        "changes": [{"insert": {"id": 3, "hashes": [102, 202, 302]}}],
        "expected_version": version1  # Wrong version
    })
    assert req.status_code == 409, req.content
    assert json.loads(req.content)["error"] == "VersionMismatch"
