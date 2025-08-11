"""Tests for the HTTP proxy service"""

import asyncio
from unittest.mock import AsyncMock, Mock
from aiohttp import web
import msgspec.msgpack
import pytest

from fpindexcluster.proxy_service import ProxyService
from fpindexcluster.models import (
    SearchRequest,
    PutFingerprintRequest,
    BulkUpdateRequest,
    Change,
    Insert,
    Delete,
    EmptyResponse,
    ErrorResponse,
)


# Test configuration is now provided by conftest.py


@pytest.fixture
async def proxy_service(managed_test_resources):
    """Create a ProxyService instance for testing with managed resources"""
    resources = managed_test_resources
    service = ProxyService(resources['config'])
    
    # Use managed resources instead of creating new ones
    service.http_session = resources['http_session']
    service.nc = resources['nats_connection']
    service.js = resources['jetstream_context']
    
    main_stream_name = resources['config'].get_stream_name("main")
    print(f"Will use test stream: {main_stream_name}")
    
    yield service
    
    # Cleanup is handled automatically by conftest.py


@pytest.fixture
async def client(aiohttp_client, proxy_service):
    """Create test client"""
    return await aiohttp_client(proxy_service.app)


async def get_stream_messages(js, stream_name, subject_filter=None, max_messages=10, subject_prefix="fpindex"):
    """Helper to get messages from a NATS stream for testing"""
    from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy
    import uuid
    
    try:
        # Create a consumer with proper config object
        consumer_name = f"test-consumer-{uuid.uuid4().hex[:8]}"
        config = ConsumerConfig(
            name=consumer_name,
            deliver_policy=DeliverPolicy.ALL,
            ack_policy=AckPolicy.EXPLICIT
        )
        if subject_filter:
            config.filter_subject = subject_filter
            
        consumer = await js.add_consumer(stream_name, config=config)
        
        messages = []
        try:
            # Use pull consumer to get messages
            # Stream accepts subjects like "{prefix}.main.>" regardless of stream name
            pull_sub = await js.pull_subscribe(
                f"{subject_prefix}.>",  # Subscribe to all subjects with this prefix
                stream=stream_name,  # But only from this specific stream
                durable=consumer_name
            )
            
            # Fetch messages with timeout
            msgs = await pull_sub.fetch(max_messages, timeout=1.0)
            for msg in msgs:
                messages.append({
                    "subject": msg.subject,
                    "data": msg.data,
                    "headers": msg.headers or {}
                })
                await msg.ack()
                
            # Clean up subscription
            await pull_sub.unsubscribe()
            
        except Exception as e:
            # Timeout or other error is expected when no messages
            print(f"Fetch error (expected if no messages): {e}")
            pass
        
        # Clean up consumer
        try:
            await js.delete_consumer(stream_name, consumer_name)
        except Exception:
            pass
            
        return messages
    except Exception as e:
        print(f"Error getting stream messages: {e}")
        return []


async def wait_for_message(js, stream_name, subject_filter, timeout=3.0, subject_prefix="fpindex"):
    """Wait for a specific message to appear in the stream"""
    import time
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        messages = await get_stream_messages(js, stream_name, subject_filter, 1, subject_prefix)
        if messages:
            return messages[0]
        await asyncio.sleep(0.2)
    
    # Try one more time without filter to see all messages
    all_messages = await get_stream_messages(js, stream_name, None, 10, subject_prefix)
    print(f"All messages when looking for {subject_filter}: {[msg['subject'] for msg in all_messages]}")
    return None

async def test_health_endpoint(client):
    """Test global health check"""
    resp = await client.get("/_health")

    assert resp.status == 200
    assert resp.headers["Content-Type"] == "text/plain; charset=utf-8"
    text = await resp.text()
    assert text == "OK\n"

async def test_index_health_endpoint(client):
    """Test index-specific health check"""
    resp = await client.get("/main/_health")

    assert resp.status == 200
    assert resp.headers["Content-Type"] == "text/plain; charset=utf-8"
    text = await resp.text()
    assert text == "OK\n"

async def test_create_index(client):
    """Test index creation"""
    resp = await client.put("/main")

    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    # Should return EmptyResponse
    body = await resp.read()
    decoded = msgspec.msgpack.decode(body)
    assert decoded == {}

async def test_delete_index(client):
    """Test index deletion"""
    # First create the index so we can delete it
    create_resp = await client.put("/main")
    assert create_resp.status == 200
    
    # Now delete it
    resp = await client.delete("/main")

    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    # Should return EmptyResponse
    body = await resp.read()
    decoded = msgspec.msgpack.decode(body)
    assert decoded == {}

async def test_get_index_info_existing_stream(client):
    """Test GET index info returns version from stream"""
    # First create the index so it exists
    create_resp = await client.put("/main")
    assert create_resp.status == 200
    
    # Now get info for the existing stream
    resp = await client.get("/main")

    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    body = await resp.read()
    decoded = msgspec.msgpack.decode(body)
    # Stream exists and is empty, version should be 0
    assert "v" in decoded
    assert isinstance(decoded["v"], int)
    assert decoded["v"] == 0  # New empty stream

async def test_get_index_info_nonexistent_stream(client):
    """Test GET index info returns 404 for nonexistent stream"""
    resp = await client.get("/nonexistent")

    assert resp.status == 404
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    body = await resp.read()
    decoded = msgspec.msgpack.decode(body)
    assert decoded == {"e": "Index not found"}

async def test_get_index_info_version_increases_after_updates(client):
    """Test that index version increases after adding fingerprints"""
    # First create the index
    create_resp = await client.put("/main")
    assert create_resp.status == 200
    
    # Get initial version (should be 0 for new empty stream)
    resp = await client.get("/main")
    assert resp.status == 200
    body = await resp.read()
    initial_version = msgspec.msgpack.decode(body)["v"]
    assert initial_version == 0
    
    # Add a fingerprint
    request_data = PutFingerprintRequest(hashes=[1001, 2002, 3003])
    body = msgspec.msgpack.encode(request_data)
    resp = await client.put("/main/123", data=body, headers={"Content-Type": "application/vnd.msgpack"})
    assert resp.status == 200
    
    # Wait a bit for message to be processed
    await asyncio.sleep(0.1)
    
    # Get version again
    resp = await client.get("/main")
    assert resp.status == 200
    body = await resp.read()
    new_version = msgspec.msgpack.decode(body)["v"]
    
    # Version should have increased (should be 1 after one message)
    assert new_version > initial_version
    assert new_version == 1

async def test_get_fingerprint_not_implemented(client):
    """Test GET fingerprint returns not implemented"""
    resp = await client.get("/main/123456789")

    assert resp.status == 501
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    body = await resp.read()
    decoded = msgspec.msgpack.decode(body)
    assert decoded == {"e": "NotImplemented"}

async def test_put_fingerprint_valid(client, proxy_service):
    """Test PUT fingerprint with valid data"""
    # Prepare request data
    request_data = PutFingerprintRequest(hashes=[1001, 2002, 3003])
    body = msgspec.msgpack.encode(request_data)

    resp = await client.put(
        "/main/123456789",
        data=body,
        headers={"Content-Type": "application/vnd.msgpack"}
    )

    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    # Should return EmptyResponse
    response_body = await resp.read()
    decoded = msgspec.msgpack.decode(response_body)
    assert decoded == {}

    # Wait a bit for message to be published
    await asyncio.sleep(0.5)
    
    # Debug: check what stream the proxy service is actually using
    actual_stream_name = proxy_service.config.get_stream_name("main")
    print(f"Proxy service stream name: {actual_stream_name}")
    print(f"Config prefix: {proxy_service.config.nats_stream_prefix}")
    
    # Debug: check all messages in the stream
    all_messages = await get_stream_messages(proxy_service.js, actual_stream_name, subject_prefix=proxy_service.config.get_subject_prefix())
    print(f"All messages in stream: {len(all_messages)}")
    for msg in all_messages:
        print(f"Subject: {msg['subject']}, Data length: {len(msg['data'])}")
    
    # Verify message was published to NATS stream
    expected_subject = f"{proxy_service.config.get_subject_prefix()}.main.075bcd15"
    message = await wait_for_message(proxy_service.js, actual_stream_name, expected_subject, subject_prefix=proxy_service.config.get_subject_prefix())
    
    assert message is not None, f"Message should have been published to NATS. Expected subject: {expected_subject}. Found messages: {[msg['subject'] for msg in all_messages]}"
    assert message["subject"] == expected_subject
    
    # Check message format
    decoded_message = msgspec.msgpack.decode(message["data"])
    assert decoded_message == {"h": [1001, 2002, 3003]}

async def test_put_fingerprint_invalid_id(client, proxy_service):
    """Test PUT fingerprint with invalid ID"""
    request_data = PutFingerprintRequest(hashes=[1001, 2002, 3003])
    body = msgspec.msgpack.encode(request_data)

    resp = await client.put(
        "/main/invalid_id",
        data=body,
        headers={"Content-Type": "application/vnd.msgpack"}
    )

    assert resp.status == 400
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    response_body = await resp.read()
    decoded = msgspec.msgpack.decode(response_body)
    expected_error = "Fingerprint ID must be a 32-bit unsigned integer"
    assert decoded == {"e": expected_error}

    # Verify no message was published to NATS stream (should timeout quickly)
    actual_stream_name = proxy_service.config.get_stream_name("main")
    messages = await get_stream_messages(proxy_service.js, actual_stream_name, subject_prefix=proxy_service.config.get_subject_prefix())
    # We only expect no new messages related to invalid_id
    invalid_messages = [m for m in messages if "invalid_id" in m["subject"]]
    assert len(invalid_messages) == 0, "No messages should be published for invalid ID"

async def test_put_fingerprint_invalid_msgpack(client):
    """Test PUT fingerprint with invalid MessagePack"""
    resp = await client.put(
        "/main/123456789",
        data=b"invalid msgpack data",
        headers={"Content-Type": "application/vnd.msgpack"}
    )

    assert resp.status == 400
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    response_body = await resp.read()
    decoded = msgspec.msgpack.decode(response_body)
    assert decoded == {"e": "Invalid MessagePack"}

async def test_delete_fingerprint_valid(client, proxy_service):
    """Test DELETE fingerprint with valid ID"""
    resp = await client.delete("/main/123456789")

    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    # Should return EmptyResponse
    response_body = await resp.read()
    decoded = msgspec.msgpack.decode(response_body)
    assert decoded == {}

    # Verify message was published to NATS stream with empty body (delete)
    actual_stream_name = proxy_service.config.get_stream_name("main")
    expected_subject = f"{proxy_service.config.get_subject_prefix()}.main.075bcd15"
    message = await wait_for_message(proxy_service.js, actual_stream_name, expected_subject, subject_prefix=proxy_service.config.get_subject_prefix())
    
    assert message is not None, "Delete message should have been published to NATS"
    assert message["subject"] == expected_subject
    assert message["data"] == b""  # Empty message for delete

async def test_delete_fingerprint_invalid_id(client, proxy_service):
    """Test DELETE fingerprint with invalid ID"""
    resp = await client.delete("/main/invalid_id")

    assert resp.status == 400
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    response_body = await resp.read()
    decoded = msgspec.msgpack.decode(response_body)
    expected_error = "Fingerprint ID must be a 32-bit unsigned integer"
    assert decoded == {"e": expected_error}

    # Verify no message was published to NATS stream for invalid ID
    actual_stream_name = proxy_service.config.get_stream_name("main")
    messages = await get_stream_messages(proxy_service.js, actual_stream_name, subject_prefix=proxy_service.config.get_subject_prefix())
    invalid_messages = [m for m in messages if "invalid_id" in m["subject"]]
    assert len(invalid_messages) == 0, "No messages should be published for invalid ID"

async def test_bulk_update_valid(client, proxy_service):
    """Test bulk update with valid changes"""
    # Prepare bulk update request
    changes = [
        Change(insert=Insert(id=111, hashes=[100, 200, 300])),
        Change(insert=Insert(id=222, hashes=[400, 500, 600])),
        Change(delete=Delete(id=333))
    ]
    request_data = BulkUpdateRequest(changes=changes)
    body = msgspec.msgpack.encode(request_data)

    resp = await client.post(
        "/main/_update",
        data=body,
        headers={"Content-Type": "application/vnd.msgpack"}
    )

    # Debug: check response if not 200
    if resp.status != 200:
        response_body = await resp.read()
        decoded = msgspec.msgpack.decode(response_body)
        print(f"Error response: {decoded}")
    
    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    # Check response structure
    response_body = await resp.read()
    decoded = msgspec.msgpack.decode(response_body)

    # Should have results array with 3 items
    assert "r" in decoded  # "results" field
    results = decoded["r"]
    assert len(results) == 3

    # Check each result
    assert results[0]["i"] == 111  # id
    assert results[0]["s"] == "inserted"  # status

    assert results[1]["i"] == 222
    assert results[1]["s"] == "inserted"  # Both inserts return "inserted"

    assert results[2]["i"] == 333
    assert results[2]["s"] == "deleted"

    # Verify 3 messages were published to NATS stream
    # Wait a bit for all messages to be published
    await asyncio.sleep(0.5)
    actual_stream_name = proxy_service.config.get_stream_name("main")
    messages = await get_stream_messages(proxy_service.js, actual_stream_name, max_messages=50, subject_prefix=proxy_service.config.get_subject_prefix())
    
    # Filter messages for this bulk update (by subject patterns)
    bulk_messages = [m for m in messages if 
                    m["subject"].endswith(".0000006f") or  # 111 in hex
                    m["subject"].endswith(".000000de") or  # 222 in hex  
                    m["subject"].endswith(".0000014d")]    # 333 in hex
    
    assert len(bulk_messages) == 3, f"Expected 3 messages, got {len(bulk_messages)}"
    
    # Verify message contents
    insert_msg = next(m for m in bulk_messages if m["subject"].endswith(".0000006f"))
    decoded_insert = msgspec.msgpack.decode(insert_msg["data"])
    assert decoded_insert == {"h": [100, 200, 300]}
    
    update_msg = next(m for m in bulk_messages if m["subject"].endswith(".000000de"))
    decoded_update = msgspec.msgpack.decode(update_msg["data"])
    assert decoded_update == {"h": [400, 500, 600]}
    
    delete_msg = next(m for m in bulk_messages if m["subject"].endswith(".0000014d"))
    assert delete_msg["data"] == b""  # Empty for delete

async def test_bulk_update_invalid_msgpack(client):
    """Test bulk update with invalid MessagePack"""
    resp = await client.post(
        "/main/_update",
        data=b"invalid msgpack data",
        headers={"Content-Type": "application/vnd.msgpack"}
    )

    assert resp.status == 400
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    response_body = await resp.read()
    decoded = msgspec.msgpack.decode(response_body)
    assert decoded == {"e": "Invalid MessagePack"}

async def test_search_request_forwarding(client, proxy_service):
    """Test search request forwarding to fpindex"""
    # Mock successful response from fpindex
    mock_response = Mock()
    mock_response.status = 200
    mock_response.read = AsyncMock(
        return_value=msgspec.msgpack.encode({"r": []})
    )

    # Create a proper async context manager mock
    async_context_mock = AsyncMock()
    async_context_mock.__aenter__ = AsyncMock(return_value=mock_response)
    async_context_mock.__aexit__ = AsyncMock(return_value=None)

    proxy_service.http_session.post = Mock(
        return_value=async_context_mock
    )

    # Prepare search request
    search_request = SearchRequest(
        query=[1001, 2002, 3003], timeout=1000, limit=50
    )
    body = msgspec.msgpack.encode(search_request)

    resp = await client.post(
        "/main/_search",
        data=body,
        headers={"Content-Type": "application/vnd.msgpack"}
    )

    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    # Verify forwarding was attempted - use the actual fpindex URL from config
    expected_url = f"{proxy_service.config.fpindex_url}/main/_search"
    proxy_service.http_session.post.assert_called_once_with(
        expected_url,
        data=body,
        headers={
            "Content-Type": "application/vnd.msgpack",
            "Accept": "application/vnd.msgpack"
        }
    )

async def test_search_invalid_msgpack(client):
    """Test search with invalid MessagePack"""
    resp = await client.post(
        "/main/_search",
        data=b"invalid msgpack data",
        headers={"Content-Type": "application/vnd.msgpack"}
    )

    assert resp.status == 400
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    response_body = await resp.read()
    decoded = msgspec.msgpack.decode(response_body)
    assert decoded == {"e": "Invalid MessagePack"}

async def test_search_service_unavailable(client, proxy_service):
    """Test search when fpindex service is unavailable"""
    from unittest.mock import AsyncMock, patch
    
    # Mock the HTTP session post method to raise an exception
    with patch.object(proxy_service.http_session, 'post') as mock_post:
        mock_post.side_effect = Exception("Connection failed")

        search_request = SearchRequest(query=[1001, 2002, 3003])
        body = msgspec.msgpack.encode(search_request)

        resp = await client.post(
            "/main/_search",
            data=body,
            headers={"Content-Type": "application/vnd.msgpack"}
        )

        assert resp.status == 503
        assert resp.headers["Content-Type"] == "application/vnd.msgpack"

        response_body = await resp.read()
        decoded = msgspec.msgpack.decode(response_body)
        assert decoded == {"e": "Search service unavailable"}


def test_msgpack_response_helper(test_config):
    """Test _msgpack_response helper method"""
    proxy_service = ProxyService(test_config)
    response_struct = EmptyResponse()

    result = proxy_service._msgpack_response(response_struct)

    assert isinstance(result, web.Response)
    assert result.status == 200
    assert result.headers["Content-Type"] == "application/vnd.msgpack"

    # Check body contains encoded struct
    expected_body = msgspec.msgpack.encode(response_struct)
    assert result.body == expected_body


def test_msgpack_response_with_custom_status(test_config):
    """Test _msgpack_response with custom status code"""
    proxy_service = ProxyService(test_config)
    response_struct = ErrorResponse(error="Test error")

    result = proxy_service._msgpack_response(response_struct, 500)

    assert result.status == 500
    assert result.headers["Content-Type"] == "application/vnd.msgpack"


def test_error_response_helper(test_config):
    """Test _error_response helper method"""
    proxy_service = ProxyService(test_config)
    
    result = proxy_service._error_response("Test error message")

    assert isinstance(result, web.Response)
    assert result.status == 400
    assert result.headers["Content-Type"] == "application/vnd.msgpack"

    # Check body contains error response
    expected_struct = ErrorResponse(error="Test error message")
    expected_body = msgspec.msgpack.encode(expected_struct)
    assert result.body == expected_body


def test_error_response_with_custom_status(test_config):
    """Test _error_response with custom status code"""
    proxy_service = ProxyService(test_config)
    
    result = proxy_service._error_response("Not found", 404)

    assert result.status == 404


@pytest.mark.parametrize("id_str,expected_valid,expected_id", [
    ("0", True, 0),
    ("1", True, 1),
    ("123456789", True, 123456789),
    ("4294967295", True, 4294967295),  # Max 32-bit unsigned
])
def test_validate_fingerprint_id_valid(id_str, expected_valid, expected_id, test_config):
    """Test fingerprint ID validation with valid IDs"""
    proxy_service = ProxyService(test_config)
    
    is_valid, fp_id = proxy_service._validate_fingerprint_id(id_str)
    
    assert is_valid == expected_valid, f"Failed for {id_str}"
    if expected_valid:
        assert fp_id == expected_id, f"Wrong ID for {id_str}"


@pytest.mark.parametrize("id_str", [
    "invalid",
    "-1",
    "4294967296",  # Exceeds 32-bit unsigned max
    "12.34",
    "",
    "0x123",
])
def test_validate_fingerprint_id_invalid(id_str, test_config):
    """Test fingerprint ID validation with invalid IDs"""
    proxy_service = ProxyService(test_config)
    
    is_valid, _ = proxy_service._validate_fingerprint_id(id_str)
    
    assert not is_valid, f"Should be invalid: {id_str}"


if __name__ == "__main__":
    pytest.main([__file__])
