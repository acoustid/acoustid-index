"""Tests for the updater service"""

import asyncio
import os
import time
import uuid
from unittest.mock import AsyncMock, Mock
import msgspec.msgpack
import pytest

from fpindexcluster.updater_service import UpdaterService
from fpindexcluster.models import FingerprintData, ChangeInsert, ChangeDelete, Insert, Delete, BulkUpdateRequest


class UpdaterTestConfig:
    """Test configuration"""

    def __init__(self):
        self.nats_url = os.getenv("TEST_NATS_URL", "nats://localhost:4222")
        self.nats_stream = "fpindex-test"
        self.nats_stream_prefix = "fpindex-test"
        self.fpindex_url = os.getenv("TEST_FPINDEX_URL", "http://localhost:6081")
        self.consumer_name = f"test-consumer-{uuid.uuid4().hex[:8]}"
    
    def get_stream_name(self, index_name: str) -> str:
        """Get stream name for a specific index"""
        return f"{self.nats_stream_prefix}-{index_name}"


@pytest.fixture
async def test_config():
    """Create test config for updater service"""
    return UpdaterTestConfig()


@pytest.fixture
def updater_config():
    """Create test config for updater service"""
    return UpdaterTestConfig()


@pytest.fixture
async def updater_service(test_config):
    """Create UpdaterService with real NATS connection"""
    import nats
    from aiohttp import ClientSession
    from nats.js.api import StreamConfig, RetentionPolicy
    
    # Create unique stream name for this test
    test_stream_name = f"test-updater-{int(time.time())}-{id(test_config) % 10000}"
    test_config.nats_stream = test_stream_name
    
    service = UpdaterService(test_config)
    
    # Connect to NATS manually
    service.http_session = ClientSession()
    service.nc = await nats.connect(test_config.nats_url)
    service.js = service.nc.jetstream()
    
    # Create test stream
    stream_config = StreamConfig(
        name=test_stream_name,
        subjects=[f"{test_stream_name}.>"],
        retention=RetentionPolicy.LIMITS,
        max_msgs_per_subject=1,
        max_age=300  # 5 minutes retention
    )
    
    try:
        await service.js.add_stream(config=stream_config)
    except Exception:
        # Stream might already exist
        pass
    
    yield service
    
    # Cleanup
    try:
        await service.js.delete_stream(test_stream_name)
    except Exception:
        pass
    
    if service.http_session:
        await service.http_session.close()
    if service.nc:
        await service.nc.close()


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


@pytest.fixture
async def fpindex_session():
    """Create HTTP session and verify fpindex is available"""
    from aiohttp import ClientSession
    
    fpindex_url = os.getenv("TEST_FPINDEX_URL", "http://localhost:6081")
    
    session = ClientSession()
    
    # Ensure fpindex is available - test should fail if not
    async with session.get(f"{fpindex_url}/_health") as resp:
        assert resp.status == 200, f"fpindex not available at {fpindex_url}"
    
    yield session, fpindex_url
    
    await session.close()


async def test_bulk_update_fpindex_real(fpindex_session):
    """Test bulk update against real fpindex"""
    session, fpindex_url = fpindex_session
    
    # Create test index
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


async def test_bulk_update_fpindex_error_real(fpindex_session):
    """Test bulk update error handling against real fpindex"""
    session, fpindex_url = fpindex_session
    
    # Send invalid data to trigger error
    async with session.post(
        f"{fpindex_url}/nonexistent/_update",  # Non-existent index
        data=b"invalid msgpack",
        headers={"Content-Type": "application/vnd.msgpack"}
    ) as resp:
        # fpindex should return an error
        assert resp.status != 200


async def test_updater_service_lifecycle(test_config):
    """Test updater service start/stop lifecycle"""
    service = UpdaterService(test_config)
    
    # Initial state
    assert service.nc is None
    assert service.js is None
    assert service.http_session is None
    assert service.subscription is None
    
    # Start service
    await service.start()
    
    # Should be connected
    assert service.nc is not None
    assert service.js is not None
    assert service.http_session is not None
    
    # Stop service
    await service.stop()


def test_updater_service_init(updater_config):
    """Test UpdaterService initialization"""
    service = UpdaterService(updater_config)

    assert service.config == updater_config
    assert service.nc is None
    assert service.js is None
    assert service.http_session is None
    assert service.subscription is None


async def test_ensure_stream_exists(updater_service):
    """Test stream existence check with real NATS"""
    # The stream should exist (created by fixture)
    await updater_service._ensure_stream(updater_service.config.nats_stream)
    
    # Should not raise any exception
    # Stream info should be accessible
    stream_info = await updater_service.js.stream_info(updater_service.config.nats_stream)
    assert stream_info is not None
    assert stream_info.config.name == updater_service.config.nats_stream


async def test_ensure_stream_not_exists(updater_service):
    """Test handling when stream doesn't exist"""
    nonexistent_stream = f"nonexistent-{uuid.uuid4().hex[:8]}"
    
    # Should not raise exception, just log warning
    await updater_service._ensure_stream(nonexistent_stream)
    
    # Verify stream doesn't actually exist
    with pytest.raises(Exception):
        await updater_service.js.stream_info(nonexistent_stream)


async def test_ensure_stream_exists_mock(mock_updater_service):
    """Test stream existence check"""
    # Mock stream info response
    mock_stream_info = Mock()
    mock_stream_info.state.messages = 42

    mock_updater_service.js.stream_info.return_value = mock_stream_info

    await mock_updater_service._ensure_stream("test-stream")

    mock_updater_service.js.stream_info.assert_called_once_with("test-stream")


async def test_ensure_stream_not_exists_mock(mock_updater_service):
    """Test handling when stream doesn't exist"""
    # Mock stream not found
    mock_updater_service.js.stream_info.side_effect = Exception("stream not found")

    # Should not raise exception, just log warning
    await mock_updater_service._ensure_stream("test-stream")

    mock_updater_service.js.stream_info.assert_called_once_with("test-stream")


async def test_message_processing_integration(updater_service, fpindex_session):
    """Test end-to-end message processing with real NATS and fpindex"""
    session, fpindex_url = fpindex_session
    
    # Create test index in fpindex
    async with session.put(f"{fpindex_url}/testindex") as resp:
        assert resp.status == 200
    
    # Publish test messages to NATS stream
    test_messages = [
        {
            "subject": f"{updater_service.config.nats_stream}.testindex.000001f4",  # ID 500
            "data": msgspec.msgpack.encode(FingerprintData(hashes=[1001, 2002, 3003]))
        },
        {
            "subject": f"{updater_service.config.nats_stream}.testindex.000003e8",  # ID 1000
            "data": b""  # Delete message
        }
    ]
    
    # Publish messages
    for msg in test_messages:
        await updater_service.js.publish(msg["subject"], msg["data"])
    
    # Let updater process the messages by starting message loop briefly
    # We'll create a consumer manually to verify the processing would work
    from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy
    
    consumer_config = ConsumerConfig(
        name=f"test-consumer-{uuid.uuid4().hex[:8]}",
        deliver_policy=DeliverPolicy.ALL,
        ack_policy=AckPolicy.EXPLICIT,
    )
    
    await updater_service.js.add_consumer(updater_service.config.nats_stream, config=consumer_config)
    
    subscription = await updater_service.js.pull_subscribe(
        f"{updater_service.config.nats_stream}.>", 
        durable=consumer_config.name
    )
    
    # Fetch and process messages
    messages = await subscription.fetch(batch=10, timeout=2.0)
    assert len(messages) == 2
    
    # Verify message parsing logic
    for msg in messages:
        subject_parts = msg.subject.split(".")
        assert len(subject_parts) == 3
        _, index_name, fpid_hex = subject_parts
        assert index_name == "testindex"
        
        fp_id = int(fpid_hex, 16)
        assert fp_id in [500, 1000]
        
        # Ack message
        await msg.ack()
    
    await subscription.unsubscribe()


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


def test_bulk_update_request_creation():
    """Test creating BulkUpdateRequest objects"""
    changes = [
        ChangeInsert(insert=Insert(id=123, hashes=[100, 200])),
        ChangeDelete(delete=Delete(id=456))
    ]
    
    bulk_request = BulkUpdateRequest(changes=changes)
    
    # Encode and decode to verify structure
    encoded = msgspec.msgpack.encode(bulk_request)
    decoded = msgspec.msgpack.decode(encoded)
    
    assert "c" in decoded  # changes field
    assert len(decoded["c"]) == 2
    
    # Check insert change
    insert_change = decoded["c"][0]
    assert "i" in insert_change
    assert insert_change["i"]["i"] == 123  # id field
    assert insert_change["i"]["h"] == [100, 200]  # hashes field
    
    # Check delete change
    delete_change = decoded["c"][1]
    assert "d" in delete_change
    assert delete_change["d"]["i"] == 456  # id field


async def test_updater_service_real_nats():
    """Test updater service with real NATS integration"""
    import os
    import time
    import nats
    from nats.js.api import StreamConfig, RetentionPolicy
    from aiohttp import ClientSession

    nats_url = os.getenv("TEST_NATS_URL", "nats://localhost:4222")
    fpindex_url = os.getenv("TEST_FPINDEX_URL", "http://localhost:6081")

    # Skip if NATS not available
    try:
        nc = await nats.connect(nats_url)
        await nc.close()
    except Exception:
        pytest.skip("NATS not available")

    # Skip if fpindex not available
    try:
        async with ClientSession() as session:
            async with session.get(f"{fpindex_url}/_health") as resp:
                if resp.status != 200:
                    pytest.skip("fpindex not available")
    except Exception:
        pytest.skip("fpindex not available")

    # Create test stream and setup
    stream_name = f"test-updater-{int(time.time())}"

    # Setup test stream and publish test message
    nc = await nats.connect(nats_url)
    js = nc.jetstream()

    try:
        # Create test stream
        stream_config = StreamConfig(
            name=stream_name,
            subjects=[f"{stream_name}.>"],
            retention=RetentionPolicy.LIMITS,
            max_msgs_per_subject=1,
            max_age=300,  # 5 minutes
        )
        await js.add_stream(config=stream_config)

        # Create test index in fpindex
        async with ClientSession() as session:
            async with session.put(f"{fpindex_url}/testindex") as resp:
                assert resp.status == 200

        # Create updater service
        config = UpdaterTestConfig()
        config.nats_url = nats_url
        config.nats_stream = stream_name
        config.fpindex_url = fpindex_url
        config.consumer_name = f"test-consumer-{int(time.time())}"

        updater = UpdaterService(config)

        # Start updater (this will create consumer and subscription)
        await updater.start()

        # Publish test message
        test_fp_id = 555666777
        test_hashes = [9001, 9002, 9003]

        subject = f"{stream_name}.testindex.{test_fp_id:08x}"
        message_data = msgspec.msgpack.encode(FingerprintData(hashes=test_hashes))

        await js.publish(subject, message_data)

        # Wait for message processing
        await asyncio.sleep(3)

        # Verify fingerprint was processed by checking fpindex
        async with ClientSession() as session:
            async with session.get(f"{fpindex_url}/testindex/{test_fp_id}") as resp:
                assert resp.status == 200  # Should exist

        # Test delete message
        await js.publish(subject, b"")  # Empty message = delete
        await asyncio.sleep(2)

        # Verify fingerprint was deleted
        async with ClientSession() as session:
            async with session.get(f"{fpindex_url}/testindex/{test_fp_id}") as resp:
                assert resp.status == 404  # Should be gone

        await updater.stop()

    finally:
        # Cleanup
        try:
            await js.delete_stream(stream_name)
        except Exception:
            pass
        await nc.close()


async def test_message_batching_real_nats():
    """Test message batching with real NATS"""
    import os
    import time
    import nats
    from nats.js.api import StreamConfig, RetentionPolicy
    from aiohttp import ClientSession

    nats_url = os.getenv("TEST_NATS_URL", "nats://localhost:4222")
    fpindex_url = os.getenv("TEST_FPINDEX_URL", "http://localhost:6081")

    # Skip if services not available
    try:
        nc = await nats.connect(nats_url)
        await nc.close()
    except Exception:
        pytest.skip("NATS not available")

    try:
        async with ClientSession() as session:
            async with session.get(f"{fpindex_url}/_health") as resp:
                if resp.status != 200:
                    pytest.skip("fpindex not available")
    except Exception:
        pytest.skip("fpindex not available")

    stream_name = f"test-batch-{int(time.time())}"

    nc = await nats.connect(nats_url)
    js = nc.jetstream()

    try:
        # Create test stream
        stream_config = StreamConfig(
            name=stream_name,
            subjects=[f"{stream_name}.>"],
            retention=RetentionPolicy.LIMITS,
            max_msgs_per_subject=1,
            max_age=300,
        )
        await js.add_stream(config=stream_config)

        # Create test index
        async with ClientSession() as session:
            async with session.put(f"{fpindex_url}/batchtest") as resp:
                assert resp.status == 200

        # Create updater
        config = UpdaterTestConfig()
        config.nats_url = nats_url
        config.nats_stream = stream_name
        config.fpindex_url = fpindex_url
        config.consumer_name = f"batch-consumer-{int(time.time())}"

        updater = UpdaterService(config)
        await updater.start()

        # Publish multiple messages quickly to test batching
        test_fps = [
            (888111, [1100, 1200, 1300]),
            (888222, [1400, 1500, 1600]),
            (888333, [1700, 1800, 1900]),
            (888444, [2000, 2100, 2200]),
            (888555, [2300, 2400, 2500]),
        ]

        for fp_id, hashes in test_fps:
            subject = f"{stream_name}.batchtest.{fp_id:08x}"
            message_data = msgspec.msgpack.encode(FingerprintData(hashes=hashes))
            await js.publish(subject, message_data)

        # Wait for batch processing
        await asyncio.sleep(4)

        # Verify all fingerprints were processed
        async with ClientSession() as session:
            for fp_id, _ in test_fps:
                async with session.get(f"{fpindex_url}/batchtest/{fp_id}") as resp:
                    assert resp.status == 200, f"Fingerprint {fp_id} should exist"

        await updater.stop()

    finally:
        try:
            await js.delete_stream(stream_name)
        except Exception:
            pass
        await nc.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
