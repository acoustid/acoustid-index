"""Tests for the HTTP proxy service"""

import os
from unittest.mock import AsyncMock, Mock
from aiohttp import web
import msgspec.msgpack
import pytest
import asyncio

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


class ProxyTestConfig:
    """Test configuration with environment variable support"""
    def __init__(self):
        self.proxy_host = os.getenv("TEST_PROXY_HOST", "127.0.0.1")
        self.proxy_port = int(os.getenv("TEST_PROXY_PORT", "8080"))
        self.nats_url = os.getenv("TEST_NATS_URL", "nats://localhost:4222")
        self.nats_stream = os.getenv("TEST_NATS_STREAM", "fpindex-test")
        self.fpindex_url = os.getenv("TEST_FPINDEX_URL", "http://localhost:8081")


@pytest.fixture
async def proxy_service():
    """Create a ProxyService instance for testing with real NATS"""
    import nats
    from aiohttp import ClientSession
    
    config = ProxyTestConfig()
    service = ProxyService(config)
    
    # Connect to NATS manually instead of using service.start() to avoid HTTP server
    service.http_session = ClientSession()
    service.nc = await nats.connect(config.nats_url)
    service.js = service.nc.jetstream()
    
    # Create a test stream with unique name
    import time
    from nats.js.api import StreamConfig, RetentionPolicy
    
    test_stream_name = f"test-{int(time.time())}-{id(service) % 10000}"
    
    # Create stream with proper configuration matching production setup
    stream_config = StreamConfig(
        name=test_stream_name,
        subjects=[f"{test_stream_name}.>"],
        retention=RetentionPolicy.LIMITS,
        max_msgs_per_subject=1,  # Enable compaction like production
        max_age=300  # 5 minutes retention for test data
    )
    
    try:
        await service.js.add_stream(config=stream_config)
        print(f"Created test stream: {test_stream_name}")
    except Exception as e:
        print(f"Stream creation error: {e}")
        # Stream might already exist, which is fine
        pass
    
    # Override the stream name to use our test stream
    original_stream = config.nats_stream
    config.nats_stream = test_stream_name
    
    yield service
    
    # Cleanup: delete test stream and stop connections
    try:
        await service.js.delete_stream(test_stream_name)
    except Exception:
        # Stream might not exist, which is fine
        pass
    
    config.nats_stream = original_stream
    if service.http_session:
        await service.http_session.close()
    if service.nc:
        await service.nc.close()


@pytest.fixture
async def client(aiohttp_client, proxy_service):
    """Create test client"""
    return await aiohttp_client(proxy_service.app)


async def get_stream_messages(js, stream_name, subject_filter=None, max_messages=10):
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
            pull_sub = await js.pull_subscribe(
                f"{stream_name}.>", 
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


async def wait_for_message(js, stream_name, subject_filter, timeout=3.0):
    """Wait for a specific message to appear in the stream"""
    import time
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        messages = await get_stream_messages(js, stream_name, subject_filter, 1)
        if messages:
            return messages[0]
        await asyncio.sleep(0.2)
    
    # Try one more time without filter to see all messages
    all_messages = await get_stream_messages(js, stream_name, None, 10)
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
    resp = await client.delete("/main")

    assert resp.status == 200
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    # Should return EmptyResponse
    body = await resp.read()
    decoded = msgspec.msgpack.decode(body)
    assert decoded == {}

async def test_get_index_info_not_implemented(client):
    """Test GET index info returns not implemented"""
    resp = await client.get("/main")

    assert resp.status == 501
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    body = await resp.read()
    decoded = msgspec.msgpack.decode(body)
    assert decoded == {"e": "NotImplemented"}

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
    
    # Debug: check all messages in the stream
    all_messages = await get_stream_messages(proxy_service.js, proxy_service.config.nats_stream)
    print(f"All messages in stream: {len(all_messages)}")
    for msg in all_messages:
        print(f"Subject: {msg['subject']}, Data length: {len(msg['data'])}")
    
    # Verify message was published to NATS stream
    expected_subject = f"{proxy_service.config.nats_stream}.main.075bcd15"
    message = await wait_for_message(proxy_service.js, proxy_service.config.nats_stream, expected_subject)
    
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
    messages = await get_stream_messages(proxy_service.js, proxy_service.config.nats_stream)
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
    expected_subject = f"{proxy_service.config.nats_stream}.main.075bcd15"
    message = await wait_for_message(proxy_service.js, proxy_service.config.nats_stream, expected_subject)
    
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
    messages = await get_stream_messages(proxy_service.js, proxy_service.config.nats_stream)
    invalid_messages = [m for m in messages if "invalid_id" in m["subject"]]
    assert len(invalid_messages) == 0, "No messages should be published for invalid ID"

async def test_bulk_update_valid(client, proxy_service):
    """Test bulk update with valid changes"""
    # Prepare bulk update request
    changes = [
        Change(insert=Insert(id=111, hashes=[100, 200, 300])),
        Change(update=Insert(id=222, hashes=[400, 500, 600])),
        Change(delete=Delete(id=333))
    ]
    request_data = BulkUpdateRequest(changes=changes)
    body = msgspec.msgpack.encode(request_data)

    resp = await client.post(
        "/main/_update",
        data=body,
        headers={"Content-Type": "application/vnd.msgpack"}
    )

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
    assert results[1]["s"] == "updated"

    assert results[2]["i"] == 333
    assert results[2]["s"] == "deleted"

    # Verify 3 messages were published to NATS stream
    # Wait a bit for all messages to be published
    await asyncio.sleep(0.5)
    messages = await get_stream_messages(proxy_service.js, proxy_service.config.nats_stream, max_messages=50)
    
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

    # Verify forwarding was attempted
    proxy_service.http_session.post.assert_called_once_with(
        "http://localhost:8081/main/_search",
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


def test_msgpack_response_helper():
    """Test _msgpack_response helper method"""
    config = ProxyTestConfig()
    proxy_service = ProxyService(config)
    response_struct = EmptyResponse()

    result = proxy_service._msgpack_response(response_struct)

    assert isinstance(result, web.Response)
    assert result.status == 200
    assert result.headers["Content-Type"] == "application/vnd.msgpack"

    # Check body contains encoded struct
    expected_body = msgspec.msgpack.encode(response_struct)
    assert result.body == expected_body


def test_msgpack_response_with_custom_status():
    """Test _msgpack_response with custom status code"""
    config = ProxyTestConfig()
    proxy_service = ProxyService(config)
    response_struct = ErrorResponse(error="Test error")

    result = proxy_service._msgpack_response(response_struct, 500)

    assert result.status == 500
    assert result.headers["Content-Type"] == "application/vnd.msgpack"


def test_error_response_helper():
    """Test _error_response helper method"""
    config = ProxyTestConfig()
    proxy_service = ProxyService(config)
    
    result = proxy_service._error_response("Test error message")

    assert isinstance(result, web.Response)
    assert result.status == 400
    assert result.headers["Content-Type"] == "application/vnd.msgpack"

    # Check body contains error response
    expected_struct = ErrorResponse(error="Test error message")
    expected_body = msgspec.msgpack.encode(expected_struct)
    assert result.body == expected_body


def test_error_response_with_custom_status():
    """Test _error_response with custom status code"""
    config = ProxyTestConfig()
    proxy_service = ProxyService(config)
    
    result = proxy_service._error_response("Not found", 404)

    assert result.status == 404


@pytest.mark.parametrize("id_str,expected_valid,expected_id", [
    ("0", True, 0),
    ("1", True, 1),
    ("123456789", True, 123456789),
    ("4294967295", True, 4294967295),  # Max 32-bit unsigned
])
def test_validate_fingerprint_id_valid(id_str, expected_valid, expected_id):
    """Test fingerprint ID validation with valid IDs"""
    config = ProxyTestConfig()
    proxy_service = ProxyService(config)
    
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
def test_validate_fingerprint_id_invalid(id_str):
    """Test fingerprint ID validation with invalid IDs"""
    config = ProxyTestConfig()
    proxy_service = ProxyService(config)
    
    is_valid, _ = proxy_service._validate_fingerprint_id(id_str)
    
    assert not is_valid, f"Should be invalid: {id_str}"


if __name__ == "__main__":
    pytest.main([__file__])
