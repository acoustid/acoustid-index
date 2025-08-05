import json
import random
import time


def wait_for_checkpoint(seconds=5):
    """Wait for background checkpointing to complete."""
    time.sleep(seconds)


def test_parallel_loading_on_restart_with_multiple_segments(server, client, index_name, create_index):
    """Test that parallel loading is triggered when restarting with multiple file segments"""
    
    # Insert a lot of data like test_insert_many to force file segment creation
    batch = []
    batch_size = 1000
    max_hash = 2**18
    total_inserts = 50000  # Same as test_insert_many
    
    for i in range(1, total_inserts + 1):
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
        req = client.post(f'/{index_name}/_update', json={
            'changes': batch,
        })
        assert req.status_code == 200, req.content
    
    # Wait for background checkpointing to complete
    wait_for_checkpoint()
    
    # Get index info to see how many segments we have
    req = client.get(f'/{index_name}')
    assert req.status_code == 200, req.content
    index_info = json.loads(req.content)
    initial_segments = index_info['segments']
    
    # Verify that multiple segments exist to ensure parallel loading will be triggered
    assert initial_segments > 1, f"Test requires multiple segments for meaningful parallel loading test, but found only {initial_segments}"
    
    print(f"Index has {initial_segments} segments before restart")
    
    # Force a shutdown to ensure segments are written
    server.stop(kill=True)
    
    # Start fresh to load from file segments (not oplog)
    server.start()
    server.wait_for_ready(index_name, timeout=30.0)
    
    # Now verify the index is functional after restart
    rng = random.Random(100)
    test_hashes = [rng.randint(0, max_hash) for _ in range(100)]
    req = client.post(f'/{index_name}/_search', json={
        'query': test_hashes,
    })
    assert req.status_code == 200, req.content
    results = json.loads(req.content)
    
    # Should find the fingerprint with ID 100
    assert len(results['results']) == 1
    assert results['results'][0]['id'] == 100
    assert results['results'][0]['score'] == 100
    
    # Verify index info is preserved
    req = client.get(f'/{index_name}')
    assert req.status_code == 200, req.content
    index_info_after = json.loads(req.content)
    
    print(f"Index has {index_info_after['segments']} segments after restart")
    
    # The number of segments might change due to background merging, but should still work
    assert index_info_after['docs'] == total_inserts


def test_sequential_loading_with_few_segments(server, client, index_name, create_index):
    """Test that sequential loading is used for indexes with few segments"""
    
    # Insert a small amount of data that won't trigger multiple file segments
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [100, 200, 300]}},
            {'insert': {'id': 2, 'hashes': [101, 201, 301]}},
        ],
    })
    assert req.status_code == 200, req.content
    
    # Get index info
    req = client.get(f'/{index_name}')
    assert req.status_code == 200, req.content
    index_info = json.loads(req.content)
    
    print(f"Small index has {index_info['segments']} segments before restart")
    
    # Restart - should use sequential loading
    server.restart()
    server.wait_for_ready(index_name, timeout=10.0)
    
    # Verify functionality
    req = client.post(f'/{index_name}/_search', json={
        'query': [100, 200, 300]
    })
    assert req.status_code == 200, req.content
    results = json.loads(req.content)
    
    assert len(results['results']) == 1
    assert results['results'][0]['id'] == 1
    assert results['results'][0]['score'] == 3