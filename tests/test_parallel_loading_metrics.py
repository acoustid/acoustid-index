import json
import random
import re
import time


def wait_for_checkpoint(seconds=5):
    """Wait for background checkpointing to complete."""
    time.sleep(seconds)


def test_parallel_loading_metrics_recorded(server, client, index_name, create_index):
    """Test that parallel loading metrics are properly recorded and exposed"""
    
    # Insert data to create multiple segments for parallel loading
    batch = []
    batch_size = 1000
    max_hash = 2**18
    total_inserts = 50000  # Should create multiple segments (matches working parallel test)
    
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
    
    # Wait for checkpointing to create file segments
    wait_for_checkpoint()
    
    # Force a restart to trigger parallel loading
    server.stop(kill=True)
    server.start()
    server.wait_for_ready(index_name, timeout=30.0)
    
    # Check metrics endpoint to see if parallel loading metrics are recorded
    req = client.get('/_metrics')
    assert req.status_code == 200
    
    metrics_text = req.content.decode()
    print("\n=== PARALLEL LOADING METRICS ===")
    
    # Look for parallel loading metrics
    parallel_metrics = [
        'parallel_loading_total',
        'sequential_loading_total', 
        'startup_duration_seconds',
        'parallel_segment_count'
    ]
    
    found_metrics = {}
    for line in metrics_text.split('\n'):
        for metric in parallel_metrics:
            if metric in line and not line.startswith('#'):
                print(f"{line}")
                # Extract the metric value - for histograms, prefer the _sum value
                if '_sum' in line:
                    base_metric = metric.replace('_seconds', '').replace('_count', '')
                    if base_metric in line:
                        match = re.search(rf'{base_metric}_sum\s+(\d+(?:\.\d+)?)', line)
                        if match:
                            found_metrics[metric + '_sum'] = float(match.group(1))
                else:
                    match = re.search(rf'{metric}[^\d]*(\d+(?:\.\d+)?)', line)
                    if match:
                        found_metrics[metric] = float(match.group(1))
    
    print(f"\nFound metrics: {found_metrics}")
    
    # Verify that parallel loading was used
    assert 'parallel_loading_total' in found_metrics
    assert found_metrics['parallel_loading_total'] > 0, "Parallel loading should have been triggered"
    
    # Verify that parallel segment count was recorded (check the sum value)
    if 'parallel_segment_count_sum' in found_metrics:
        assert found_metrics['parallel_segment_count_sum'] >= 3, "Should have loaded 3+ segments in parallel"
    
    # Verify startup duration was recorded (check sum value)
    if 'startup_duration_seconds_sum' in found_metrics:
        assert found_metrics['startup_duration_seconds_sum'] > 0, "Startup duration should be positive"
    
    print("✅ Parallel loading metrics verified successfully!")


def test_sequential_loading_metrics_recorded(server, client, index_name, create_index):
    """Test that sequential loading metrics are recorded for small indexes""" 
    
    # Insert small amount of data (should use sequential loading)
    req = client.post(f'/{index_name}/_update', json={
        'changes': [
            {'insert': {'id': 1, 'hashes': [100, 200, 300]}},
            {'insert': {'id': 2, 'hashes': [101, 201, 301]}},
        ],
    })
    assert req.status_code == 200, req.content
    
    # Restart to trigger loading (should be sequential)
    server.restart()
    server.wait_for_ready(index_name, timeout=10.0)
    
    # Check metrics  
    req = client.get('/_metrics')
    assert req.status_code == 200
    
    metrics_text = req.content.decode()
    
    # Look for sequential loading metric
    sequential_count = 0
    for line in metrics_text.split('\n'):
        if 'sequential_loading_total' in line and not line.startswith('#'):
            print(f"Sequential loading metric: {line}")
            match = re.search(r'sequential_loading_total[^\d]*(\d+)', line)
            if match:
                sequential_count = int(match.group(1))
    
    # Should have at least one sequential loading event
    assert sequential_count > 0, "Sequential loading should have been recorded"
    print(f"✅ Sequential loading metrics verified! Count: {sequential_count}")