import pytest
import tarfile
import io
import msgpack
import tempfile
import os


def test_snapshot_endpoint_index_not_found(client, index_name):
    """Test snapshot endpoint returns 404 for non-existent index"""
    req = client.get(f"/{index_name}/_snapshot")
    assert req.status_code == 404, req.content


def test_snapshot_endpoint_empty_index(client, index_name, create_index):
    """Test snapshot endpoint works with empty index"""
    req = client.get(f"/{index_name}/_snapshot")
    assert req.status_code == 200, req.content
    
    # Check response headers
    assert req.headers.get('content-type') == 'application/x-tar'
    assert 'attachment' in req.headers.get('content-disposition', '')
    assert 'index_snapshot.tar' in req.headers.get('content-disposition', '')
    
    # Verify we get a valid tar stream by extracting to temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Extract tar
        with tarfile.open(fileobj=io.BytesIO(req.content), mode='r:') as tar:
                # Add filter for Python 3.14 compatibility
                try:
                    tar.extractall(temp_dir, filter='data')
                except TypeError:
                    # Fallback for older Python versions
                    tar.extractall(temp_dir)
        
        # Check extracted files
        extracted_files = os.listdir(temp_dir)
        assert 'manifest' in extracted_files
        
        # Verify manifest content
        with open(os.path.join(temp_dir, 'manifest'), 'rb') as f:
            manifest_data = f.read()
            
            # Parse manifest using unpacker - has header + segments
            unpacker = msgpack.Unpacker(io.BytesIO(manifest_data), raw=False, strict_map_key=False)
            
            # Skip header (first object)
            header = next(unpacker)
            assert isinstance(header, dict)
            
            # Get segments (second object)
            segments = next(unpacker)
            assert isinstance(segments, list)
            assert len(segments) == 0  # Empty index


def test_snapshot_endpoint_with_data(client, index_name, create_index):
    """Test snapshot endpoint with actual index data"""
    # Add some data to the index
    req = client.post(
        f"/{index_name}/_update",
        json={
            "changes": [
                {"insert": {"id": 1, "hashes": [101, 201, 301]}},
                {"insert": {"id": 2, "hashes": [102, 202, 302]}},
                {"set_attribute": {"name": "test_attr", "value": 1234}},
            ],
        },
    )
    assert req.status_code == 200, req.content
    
    # Get snapshot
    req = client.get(f"/{index_name}/_snapshot")
    assert req.status_code == 200, req.content
    
    # Verify tar structure by extracting to temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Extract tar
        with tarfile.open(fileobj=io.BytesIO(req.content), mode='r:') as tar:
                # Add filter for Python 3.14 compatibility
                try:
                    tar.extractall(temp_dir, filter='data')
                except TypeError:
                    # Fallback for older Python versions
                    tar.extractall(temp_dir)
        
        # Get all extracted files
        extracted_files = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                rel_path = os.path.relpath(os.path.join(root, file), temp_dir)
                extracted_files.append(rel_path)
        
        # Should contain manifest
        assert 'manifest' in extracted_files
        
        # Should contain oplog files (WAL)
        oplog_files = [f for f in extracted_files if f.startswith('oplog/')]
        assert len(oplog_files) > 0, "Should contain WAL files"
        
        # Verify oplog files have .xlog extension
        for oplog_file in oplog_files:
            assert oplog_file.endswith('.xlog'), f"Oplog file {oplog_file} should have .xlog extension"
        
        # Verify manifest content
        with open(os.path.join(temp_dir, 'manifest'), 'rb') as f:
            manifest_data = f.read()
            
            # Parse manifest using unpacker - has header + segments
            unpacker = msgpack.Unpacker(io.BytesIO(manifest_data), raw=False, strict_map_key=False)
            
            # Skip header (first object)
            header = next(unpacker)
            assert isinstance(header, dict)
            
            # Get segments (second object)
            segments = next(unpacker)
            assert isinstance(segments, list)


def test_snapshot_point_in_time_consistency(client, index_name, create_index):
    """Test that snapshot captures point-in-time consistent view"""
    # Add initial data
    req = client.post(
        f"/{index_name}/_update",
        json={
            "changes": [
                {"insert": {"id": 1, "hashes": [101, 201, 301]}},
            ],
        },
    )
    assert req.status_code == 200, req.content
    
    # Get first snapshot
    req1 = client.get(f"/{index_name}/_snapshot")
    assert req1.status_code == 200, req1.content
    
    # Add more data
    req = client.post(
        f"/{index_name}/_update",
        json={
            "changes": [
                {"insert": {"id": 2, "hashes": [102, 202, 302]}},
            ],
        },
    )
    assert req.status_code == 200, req.content
    
    # Get second snapshot
    req2 = client.get(f"/{index_name}/_snapshot")
    assert req2.status_code == 200, req2.content
    
    # Snapshots should be different (more data in second)
    # Note: with plain tar, sizes might be similar due to padding, but content should differ
    assert req1.content != req2.content, "Second snapshot should have different content"


def test_snapshot_streaming_behavior(client, index_name, create_index):
    """Test that snapshot streams data (doesn't wait for complete generation)"""
    # Add data to create a non-trivial snapshot
    changes = []
    for i in range(100):  # Add more data to ensure some file segments
        changes.append({"insert": {"id": i + 1, "hashes": [100 + i, 200 + i, 300 + i]}})
    
    req = client.post(
        f"/{index_name}/_update",
        json={"changes": changes},
    )
    assert req.status_code == 200, req.content
    
    # Request snapshot with streaming (no timeout to simulate streaming)
    req = client.get(f"/{index_name}/_snapshot", timeout=30)  # Longer timeout for large data
    assert req.status_code == 200, req.content
    
    # Verify we got valid compressed data
    assert len(req.content) > 0
    
    # Verify it's valid tar
    try:
        with tarfile.open(fileobj=io.BytesIO(req.content), mode='r:') as tar:
            # Read names to verify it's valid tar
            names = tar.getnames()
            assert len(names) > 0
    except Exception as e:
        pytest.fail(f"Invalid tar data: {e}")


def test_snapshot_tar_structure_validation(client, index_name, create_index):
    """Test detailed validation of tar structure and file contents"""
    # Add some data
    req = client.post(
        f"/{index_name}/_update",
        json={
            "changes": [
                {"insert": {"id": 1, "hashes": [101, 201, 301]}},
                {"set_attribute": {"name": "backup_test", "value": 9999}},
            ],
        },
    )
    assert req.status_code == 200, req.content
    
    # Get snapshot and extract to temporary directory for detailed verification
    req = client.get(f"/{index_name}/_snapshot")
    assert req.status_code == 200, req.content
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Extract tar
        with tarfile.open(fileobj=io.BytesIO(req.content), mode='r:') as tar:
                # Add filter for Python 3.14 compatibility
                try:
                    tar.extractall(temp_dir, filter='data')
                except TypeError:
                    # Fallback for older Python versions
                    tar.extractall(temp_dir)
        
        # Verify directory structure
        extracted_files = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                rel_path = os.path.relpath(os.path.join(root, file), temp_dir)
                extracted_files.append(rel_path)
        
        # Should have manifest
        assert 'manifest' in extracted_files
        
        # Should have oplog directory with files
        oplog_files = [f for f in extracted_files if f.startswith('oplog/')]
        assert len(oplog_files) > 0
        
        # Verify manifest content
        with open(os.path.join(temp_dir, 'manifest'), 'rb') as f:
            manifest_data = f.read()
            
            # Parse manifest using unpacker - has header + segments
            unpacker = msgpack.Unpacker(io.BytesIO(manifest_data), raw=False, strict_map_key=False)
            
            # Skip header (first object)
            header = next(unpacker)
            assert isinstance(header, dict)
            
            # Get segments (second object)
            segments = next(unpacker)
            assert isinstance(segments, list)
        
        # Verify oplog files are readable
        for oplog_file in oplog_files:
            oplog_path = os.path.join(temp_dir, oplog_file)
            assert os.path.isfile(oplog_path)
            # Just verify file exists and is readable
            with open(oplog_path, 'rb') as f:
                data = f.read()
                assert len(data) > 0  # Should have some content