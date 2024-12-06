def test_health(client):
    req = client.get('/_health')
    assert req.status_code == 200, req.content


def test_index_health(client, create_index, index_name):
    req = client.get(f'/{index_name}/_health')
    assert req.status_code == 200, req.content


def test_metrics(client):
    req = client.get('/_metrics')
    assert req.status_code == 200, req.content
    assert 'aindex_searches_total' in req.text
