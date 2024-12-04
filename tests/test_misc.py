def test_ping(client):
    req = client.get(f'/_ping')
    assert req.status_code == 200, req.content
    assert 'pong' in req.text


def test_index_ping(client, index_name, create_index):
    req = client.get(f'{index_name}/_ping')
    assert req.status_code == 200, req.content
    assert 'pong' in req.text


def test_metrics(client):
    req = client.get(f'/_metrics')
    assert req.status_code == 200, req.content
    assert 'aindex_searches_total' in req.text

