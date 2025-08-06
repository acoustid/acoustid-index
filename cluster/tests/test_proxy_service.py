"""Tests for the HTTP proxy service"""

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


class MockConfig:
    """Mock configuration for testing"""
    def __init__(self):
        self.proxy_host = "127.0.0.1"
        self.proxy_port = 8080
        self.nats_url = "nats://localhost:4222"
        self.nats_stream = "fpindex"
        self.fpindex_url = "http://localhost:8081"


@pytest.fixture
async def proxy_service():
    """Create a ProxyService instance for testing"""
    config = MockConfig()
    service = ProxyService(config)
    
    # Mock NATS and HTTP session
    service.nc = AsyncMock()
    service.js = AsyncMock()
    service.http_session = AsyncMock()
    
    return service


@pytest.fixture
async def client(aiohttp_client, proxy_service):
    """Create test client"""
    return await aiohttp_client(proxy_service.app)

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

    # Verify NATS publish was called
    proxy_service.js.publish.assert_called_once()
    call_args = proxy_service.js.publish.call_args
    subject, message = call_args[0]

    # Check subject format (hex-encoded fingerprint ID)
    assert subject == "fpindex.main.075bcd15"  # 123456789 in hex

    # Check message format
    decoded_message = msgspec.msgpack.decode(message)
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

    # Verify NATS publish was NOT called
    proxy_service.js.publish.assert_not_called()

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

    # Verify NATS publish was called with empty message
    proxy_service.js.publish.assert_called_once()
    call_args = proxy_service.js.publish.call_args
    subject, message = call_args[0]

    assert subject == "fpindex.main.075bcd15"  # 123456789 in hex
    assert message == b""  # Empty message for delete

async def test_delete_fingerprint_invalid_id(client, proxy_service):
    """Test DELETE fingerprint with invalid ID"""
    resp = await client.delete("/main/invalid_id")

    assert resp.status == 400
    assert resp.headers["Content-Type"] == "application/vnd.msgpack"

    response_body = await resp.read()
    decoded = msgspec.msgpack.decode(response_body)
    expected_error = "Fingerprint ID must be a 32-bit unsigned integer"
    assert decoded == {"e": expected_error}

    # Verify NATS publish was NOT called
    proxy_service.js.publish.assert_not_called()

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

    # Verify NATS publish was called 3 times
    assert proxy_service.js.publish.call_count == 3

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
    # Mock connection error
    proxy_service.http_session.post.side_effect = Exception(
        "Connection failed"
    )

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
    config = MockConfig()
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
    config = MockConfig()
    proxy_service = ProxyService(config)
    response_struct = ErrorResponse(error="Test error")

    result = proxy_service._msgpack_response(response_struct, 500)

    assert result.status == 500
    assert result.headers["Content-Type"] == "application/vnd.msgpack"


def test_error_response_helper():
    """Test _error_response helper method"""
    config = MockConfig()
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
    config = MockConfig()
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
    config = MockConfig()
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
    config = MockConfig()
    proxy_service = ProxyService(config)
    
    is_valid, _ = proxy_service._validate_fingerprint_id(id_str)
    
    assert not is_valid, f"Should be invalid: {id_str}"


if __name__ == "__main__":
    pytest.main([__file__])
