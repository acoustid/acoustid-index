import json


def test_health(client):
    req = client.get('/_health')
    assert req.status_code == 200, req.content


def test_index_health(client, create_index, index_name):
    req = client.get(f'/{index_name}/_health')
    assert req.status_code == 200, req.content


def test_metrics(client):
    req = client.get('/_metrics')
    assert req.status_code == 200, req.content
    assert 'fpindex_searches_total' in req.text
    assert 'fpindex_search_hits_total' in req.text
    assert 'fpindex_search_misses_total' in req.text


def test_index_gauges(client, index_name, create_index):
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [100, 200, 300]}},
            {'insert': {'id': 2, 'hashes': [400, 500, 600]}},
        ],
    })
    assert req.status_code == 200, req.content
    version = json.loads(req.content)['version']

    req = client.get('/_metrics')
    assert req.status_code == 200, req.content
    assert f'fpindex_docs{{index="{index_name}"}} 2' in req.text
    assert f'fpindex_version{{index="{index_name}"}} {version}' in req.text
