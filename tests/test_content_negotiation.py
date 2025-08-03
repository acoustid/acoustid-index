import json
import msgpack
import pytest


def test_msgpack_default_no_headers(client, index_name, create_index):
    """Test that requests with no Content-Type/Accept headers default to MessagePack"""
    # Insert data without any headers - should default to MessagePack
    data = {'hashes': [101, 201, 301]}
    msgpack_data = msgpack.packb(data)
    
    req = client.put(f'/{index_name}/1', data=msgpack_data)
    assert req.status_code == 200
    
    # Response should be MessagePack (empty response)
    response_data = msgpack.loads(req.content)
    assert response_data == {}
    
    # Search without headers - should use MessagePack for both request and response
    search_data = {'query': [101, 201, 301]}
    msgpack_search = msgpack.packb(search_data)
    
    req = client.post(f'/{index_name}/_search', data=msgpack_search)
    assert req.status_code == 200
    
    # Response should be MessagePack with abbreviated keys
    response_data = msgpack.loads(req.content)
    assert response_data == {
        'r': [
            {'i': 1, 's': 3},
        ],
    }


def test_json_content_type_gets_json_response(client, index_name, create_index):
    """Test that JSON Content-Type without Accept header gets JSON response"""
    # Send JSON request without Accept header
    req = client.put(f'/{index_name}/1', 
                     json={'hashes': [101, 201, 301]},
                     headers={'Content-Type': 'application/json'})
    assert req.status_code == 200
    
    # Response should be JSON (matching request format)
    response_data = json.loads(req.content)
    assert response_data == {}
    
    # Search with JSON Content-Type, no Accept
    req = client.post(f'/{index_name}/_search',
                      json={'query': [101, 201, 301]},
                      headers={'Content-Type': 'application/json'})
    assert req.status_code == 200
    
    # Response should be JSON
    response_data = json.loads(req.content)
    assert response_data == {
        'results': [
            {'id': 1, 'score': 3},
        ],
    }


def test_msgpack_content_type_gets_msgpack_response(client, index_name, create_index):
    """Test that explicit MessagePack Content-Type gets MessagePack response"""
    data = {'hashes': [101, 201, 301]}
    msgpack_data = msgpack.packb(data)
    
    req = client.put(f'/{index_name}/1', 
                     data=msgpack_data,
                     headers={'Content-Type': 'application/vnd.msgpack'})
    assert req.status_code == 200
    
    # Response should be MessagePack
    response_data = msgpack.loads(req.content)
    assert response_data == {}


def test_mixed_formats_with_accept_header(client, index_name, create_index):
    """Test mixed request/response formats using Accept header"""
    # MessagePack request with JSON response (for debugging)
    data = {'hashes': [101, 201, 301]}
    msgpack_data = msgpack.packb(data)
    
    req = client.put(f'/{index_name}/1',
                     data=msgpack_data,
                     headers={
                         'Content-Type': 'application/vnd.msgpack',
                         'Accept': 'application/json'
                     })
    assert req.status_code == 200
    
    # Response should be JSON despite MessagePack request
    response_data = json.loads(req.content)
    assert response_data == {}
    
    # JSON request with MessagePack response
    req = client.post(f'/{index_name}/_search',
                      json={'query': [101, 201, 301]},
                      headers={
                          'Content-Type': 'application/json',
                          'Accept': 'application/vnd.msgpack'
                      })
    assert req.status_code == 200
    
    # Response should be MessagePack despite JSON request (abbreviated keys)
    response_data = msgpack.loads(req.content)
    assert response_data == {
        'r': [
            {'i': 1, 's': 3},
        ],
    }


def test_invalid_content_type_error(client, index_name, create_index):
    """Test that invalid Content-Type returns 415 error"""
    # Send raw data with invalid Content-Type to avoid client interference
    import requests
    base_url = client.base_url.rstrip('/')
    
    response = requests.put(f'{base_url}/{index_name}/1',
                           data=b'{"hashes": [101, 201, 301]}',
                           headers={'Content-Type': 'invalid/type'})
    assert response.status_code == 415


def test_invalid_accept_header_defaults_to_json(client, index_name, create_index):
    """Test that invalid Accept header gracefully defaults to JSON"""
    # Insert some data first
    req = client.put(f'/{index_name}/1', json={'hashes': [101, 201, 301]})
    assert req.status_code == 200
    
    # Request with invalid Accept header should get JSON response
    req = client.get(f'/{index_name}/1',
                     headers={'Accept': 'invalid/type'})
    assert req.status_code == 200
    
    # Response should be JSON (graceful fallback)
    response_data = json.loads(req.content)
    assert response_data == {'version': 1}


def test_error_responses_match_request_format(client, index_name, create_index):
    """Test that error responses use the same format as request"""
    import requests
    base_url = client.base_url.rstrip('/')
    
    # Test 1: MessagePack request gets MessagePack error
    response = requests.put(f'{base_url}/{index_name}/1',
                           data=b'invalid msgpack data',
                           headers={'Content-Type': 'application/vnd.msgpack'})
    assert response.status_code == 400  # Bad request due to invalid msgpack
    
    # Error response should be MessagePack
    assert response.headers['content-type'] == 'application/vnd.msgpack'
    error_data = msgpack.loads(response.content)
    assert 'error' in error_data or 'e' in error_data  # Could be abbreviated
    
    # Test 2: JSON request gets JSON error  
    response = requests.put(f'{base_url}/{index_name}/2',
                           data=b'invalid json data',
                           headers={'Content-Type': 'application/json'})
    assert response.status_code == 400  # Bad request due to invalid json
    
    # Error response should be JSON
    assert response.headers['content-type'] == 'application/json'
    error_data = json.loads(response.content)
    assert 'error' in error_data
    
    # Test 3: No headers request gets MessagePack error (body present = msgpack default)
    response = requests.put(f'{base_url}/{index_name}/3',
                           data=b'invalid data')
    assert response.status_code == 400  # Bad request due to invalid msgpack
    
    # Error response should be MessagePack (matches default)
    assert response.headers['content-type'] == 'application/vnd.msgpack'
    error_data = msgpack.loads(response.content)
    assert 'error' in error_data or 'e' in error_data


def test_minimal_headers_performance_case(client, index_name, create_index):
    """Test the optimal performance case - no headers needed for MessagePack"""
    # This is the target use case: minimal headers for high-performance MessagePack
    data = {'hashes': [101, 201, 301]}
    msgpack_data = msgpack.packb(data)
    
    # No Content-Type header needed
    req = client.put(f'/{index_name}/1', data=msgpack_data)
    assert req.status_code == 200
    
    # Search also with minimal headers
    search_data = {'query': [101, 201, 301]}
    msgpack_search = msgpack.packb(search_data)
    
    req = client.post(f'/{index_name}/_search', data=msgpack_search)
    assert req.status_code == 200
    
    # Verify we get the correct MessagePack response (abbreviated keys)
    response_data = msgpack.loads(req.content)
    assert response_data == {
        'r': [
            {'i': 1, 's': 3},
        ],
    }