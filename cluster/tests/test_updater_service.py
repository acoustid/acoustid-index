"""Tests for the updater service"""

import asyncio
import os
from unittest.mock import AsyncMock, Mock, patch
import msgspec.msgpack
import pytest

from fpindexcluster.updater_service import UpdaterService
from fpindexcluster.models import FingerprintData


class UpdaterTestConfig:
    """Test configuration"""
    def __init__(self):
        self.nats_url = os.getenv("TEST_NATS_URL", "nats://localhost:4222")
        self.nats_stream = os.getenv("TEST_NATS_STREAM", "fpindex-test")
        self.fpindex_url = os.getenv("TEST_FPINDEX_URL", "http://localhost:8081")
        self.consumer_name = "test-consumer"


@pytest.fixture
def updater_config():
    """Create test config for updater service"""
    return UpdaterTestConfig()


@pytest.fixture
def mock_updater_service(updater_config):
    """Create UpdaterService with mocked dependencies"""
    service = UpdaterService(updater_config)
    
    # Mock NATS connection
    service.nc = AsyncMock()
    service.js = AsyncMock()
    service.subscription = AsyncMock()
    
    # Mock HTTP session
    service.http_session = AsyncMock()
    
    return service


def test_updater_service_init(updater_config):
    """Test UpdaterService initialization"""
    service = UpdaterService(updater_config)
    
    assert service.config == updater_config
    assert service.nc is None
    assert service.js is None
    assert service.http_session is None
    assert service.subscription is None


async def test_bulk_update_fpindex_real():
    """Test bulk update against real fpindex"""
    import os
    from aiohttp import ClientSession
    from fpindexcluster.models import ChangeInsert, ChangeDelete, Insert, Delete, BulkUpdateRequest
    
    fpindex_url = os.getenv("TEST_FPINDEX_URL", "http://localhost:6081")
    
    # Skip if fpindex not available
    try:
        async with ClientSession() as session:
            async with session.get(f"{fpindex_url}/_health") as resp:
                if resp.status != 200:
                    pytest.skip("fpindex not available")
    except Exception:
        pytest.skip("fpindex not available")
    
    # Create test index
    async with ClientSession() as session:
        async with session.put(f"{fpindex_url}/test") as resp:
            assert resp.status == 200
        
        # Test bulk update
        changes = [
            ChangeInsert(insert=Insert(id=123, hashes=[100, 200, 300])),
            ChangeInsert(insert=Insert(id=456, hashes=[400, 500, 600])),
            ChangeDelete(delete=Delete(id=789))  # Delete non-existent ID (should be fine)
        ]
        
        bulk_request = BulkUpdateRequest(changes=changes)
        request_data = msgspec.msgpack.encode(bulk_request)
        
        async with session.post(
            f"{fpindex_url}/test/_update",
            data=request_data,
            headers={"Content-Type": "application/vnd.msgpack"}
        ) as resp:
            assert resp.status == 200
            # fpindex /_update returns empty response
            response_body = await resp.read()
            decoded = msgspec.msgpack.decode(response_body)
            assert decoded == {}


async def test_bulk_update_fpindex_error_real():
    """Test bulk update error handling against real fpindex"""
    import os
    from aiohttp import ClientSession
    
    fpindex_url = os.getenv("TEST_FPINDEX_URL", "http://localhost:6081")
    
    # Skip if fpindex not available
    try:
        async with ClientSession() as session:
            async with session.get(f"{fpindex_url}/_health") as resp:
                if resp.status != 200:
                    pytest.skip("fpindex not available")
    except Exception:
        pytest.skip("fpindex not available")
    
    async with ClientSession() as session:
        # Send invalid data to trigger error
        async with session.post(
            f"{fpindex_url}/nonexistent/_update",  # Non-existent index
            data=b"invalid msgpack",
            headers={"Content-Type": "application/vnd.msgpack"}
        ) as resp:
            # fpindex should return an error
            assert resp.status != 200


def test_message_parsing():
    """Test message subject parsing logic"""
    # Test valid subject
    subject = "fpindex.main.075bcd15"
    parts = subject.split(".")
    
    assert len(parts) == 3
    _, index_name, fpid_hex = parts
    assert index_name == "main"
    assert fpid_hex == "075bcd15"
    
    # Convert hex back to int
    fp_id = int(fpid_hex, 16)
    assert fp_id == 123456789


def test_fingerprint_data_encoding():
    """Test FingerprintData encoding/decoding"""
    hashes = [1001, 2002, 3003]
    fp_data = FingerprintData(hashes=hashes)
    
    # Encode to msgpack
    encoded = msgspec.msgpack.encode(fp_data)
    
    # Decode back
    decoded = msgspec.msgpack.decode(encoded, type=FingerprintData)
    
    assert decoded.hashes == hashes


async def test_ensure_stream_exists(mock_updater_service):
    """Test stream existence check"""
    # Mock stream info response
    mock_stream_info = Mock()
    mock_stream_info.state.messages = 42
    
    mock_updater_service.js.stream_info.return_value = mock_stream_info
    
    await mock_updater_service._ensure_stream("test-stream")
    
    mock_updater_service.js.stream_info.assert_called_once_with("test-stream")


async def test_ensure_stream_not_exists(mock_updater_service):
    """Test handling when stream doesn't exist"""
    # Mock stream not found
    mock_updater_service.js.stream_info.side_effect = Exception("stream not found")
    
    # Should not raise exception, just log warning
    await mock_updater_service._ensure_stream("test-stream")
    
    mock_updater_service.js.stream_info.assert_called_once_with("test-stream")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])