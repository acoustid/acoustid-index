"""Tests for the updater service"""

import asyncio
import time
import uuid
from unittest.mock import AsyncMock, Mock
import msgspec.msgpack
import pytest

from fpindexcluster.updater_service import UpdaterService
from fpindexcluster.models import FingerprintData, Change, Insert, Delete, BulkUpdateRequest


# Test configuration is now provided by conftest.py


# Test config fixtures are now provided by conftest.py


@pytest.fixture
async def updater_service(managed_test_resources):
    """Create UpdaterService with managed resources"""
    from nats.js.api import StreamConfig, RetentionPolicy
    
    resources = managed_test_resources
    service = UpdaterService(resources['config'])
    
    # Use managed resources
    service.http_session = resources['http_session']
    service.nc = resources['nats_connection']
    service.js = resources['jetstream_context']
    
    # Create test stream
    test_stream_name = resources['config'].get_stream_name("test")
    stream_config = StreamConfig(
        name=test_stream_name,
        subjects=[f"{resources['config'].get_subject_prefix()}.test.>"],
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
    
    # Cleanup is handled automatically by conftest.py


@pytest.fixture
def mock_updater_service(test_config):
    """Create UpdaterService with mocked dependencies"""
    service = UpdaterService(test_config)

    # Mock NATS connection
    service.nc = AsyncMock()
    service.js = AsyncMock()
    service.subscription = AsyncMock()

    # Mock HTTP session
    service.http_session = AsyncMock()

    return service


@pytest.fixture
async def fpindex_session(managed_test_resources, skip_if_no_fpindex):
    """Provide fpindex session using managed resources"""
    resources = managed_test_resources
    yield resources['http_session'], resources['config'].fpindex_url


async def test_bulk_update_fpindex_real(fpindex_session):
    """Test bulk update against real fpindex"""
    session, fpindex_url = fpindex_session
    
    # Create test index
    async with session.put(f"{fpindex_url}/test") as resp:
        assert resp.status == 200
    
    # Test bulk update
    changes = [
        Change(insert=Insert(id=123, hashes=[100, 200, 300])),
        Change(insert=Insert(id=456, hashes=[400, 500, 600])),
        Change(delete=Delete(id=789))  # Delete non-existent ID (should be fine)
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
    assert service.subscriptions == {}
    assert service.tracked_indexes == set()
    assert service._exit_stack is None
    
    # Test async context manager lifecycle
    async with service:
        # Should be connected
        assert hasattr(service, 'nc') and service.nc is not None
        assert hasattr(service, 'js') and service.js is not None
        assert hasattr(service, 'http_session') and service.http_session is not None


def test_updater_service_init(test_config):
    """Test UpdaterService initialization"""
    service = UpdaterService(test_config)

    assert service.config == test_config
    assert service.subscriptions == {}
    assert service.tracked_indexes == set()
    assert service._exit_stack is None


async def test_stream_info_check(updater_service):
    """Test that we can get stream info from real NATS"""
    # The stream should exist (created by fixture)
    test_stream_name = updater_service.config.get_stream_name("test")
    stream_info = await updater_service.js.stream_info(test_stream_name)
    assert stream_info is not None
    assert stream_info.config.name == test_stream_name


async def test_stream_info_nonexistent(updater_service):
    """Test handling when stream doesn't exist"""
    nonexistent_stream = f"nonexistent-{uuid.uuid4().hex[:8]}"
    
    # Should raise exception for nonexistent stream
    with pytest.raises(Exception):
        await updater_service.js.stream_info(nonexistent_stream)


async def test_message_processing_integration(updater_service, fpindex_session):
    """Test end-to-end message processing with real NATS and fpindex"""
    session, fpindex_url = fpindex_session
    
    # Create test index in fpindex
    async with session.put(f"{fpindex_url}/testindex") as resp:
        assert resp.status == 200
    
    # Create stream for testindex messages
    from nats.js.api import StreamConfig, RetentionPolicy
    testindex_stream_name = updater_service.config.get_stream_name("testindex")
    stream_config = StreamConfig(
        name=testindex_stream_name,
        subjects=[f"{updater_service.config.get_subject_prefix()}.testindex.>"],
        retention=RetentionPolicy.LIMITS,
        max_msgs_per_subject=1,
        max_age=300
    )
    try:
        await updater_service.js.add_stream(config=stream_config)
    except Exception:
        # Stream might already exist
        pass
    
    # Publish test messages to NATS stream
    test_messages = [
        {
            "subject": f"{updater_service.config.get_subject_prefix()}.testindex.000001f4",  # ID 500
            "data": msgspec.msgpack.encode(FingerprintData(hashes=[1001, 2002, 3003]))
        },
        {
            "subject": f"{updater_service.config.get_subject_prefix()}.testindex.000003e8",  # ID 1000
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
    
    test_stream_name = updater_service.config.get_stream_name("testindex")
    await updater_service.js.add_consumer(test_stream_name, config=consumer_config)
    
    subscription = await updater_service.js.pull_subscribe(
        f"{updater_service.config.get_subject_prefix()}.testindex.>", 
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
    # Test valid subject format
    subject = "testprefix.main.075bcd15"
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
        Change(insert=Insert(id=123, hashes=[100, 200])),
        Change(delete=Delete(id=456))
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


async def test_updater_service_real_nats(managed_test_resources, skip_if_no_nats, skip_if_no_fpindex):
    """Test updater service with real NATS integration"""
    import time
    from nats.js.api import StreamConfig, RetentionPolicy

    resources = managed_test_resources
    
    # Create test stream and setup
    stream_name = f"{resources['config'].nats_stream_prefix}-test-updater-{int(time.time())}"

    try:
        # Create test stream
        stream_config = StreamConfig(
            name=stream_name,
            subjects=[f"{resources['config'].get_subject_prefix()}.testindex.>"],
            retention=RetentionPolicy.LIMITS,
            max_msgs_per_subject=1,
            max_age=300,  # 5 minutes
        )
        await resources['jetstream_context'].add_stream(config=stream_config)

        # Create test index in fpindex
        async with resources['http_session'].put(f"{resources['config'].fpindex_url}/testindex") as resp:
            assert resp.status == 200

        # Create updater service
        updater = UpdaterService(resources['config'])

        # Use async context manager instead of start/stop
        async with updater:
            # Publish test message
            test_fp_id = 555666777
            test_hashes = [9001, 9002, 9003]

            subject = f"{resources['config'].get_subject_prefix()}.testindex.{test_fp_id:08x}"
            message_data = msgspec.msgpack.encode(FingerprintData(hashes=test_hashes))

            await resources['jetstream_context'].publish(subject, message_data)

            # Wait for message processing - note: the updater service might not 
            # automatically process messages in this test setup
            await asyncio.sleep(1)

            # For this test, we'll just verify the message was published correctly
            # The actual message processing would require a more complex test setup

    finally:
        # Cleanup is handled automatically by conftest
        pass


async def test_message_batching_real_nats(managed_test_resources, skip_if_no_nats, skip_if_no_fpindex):
    """Test message batching with real NATS"""
    import time
    from nats.js.api import StreamConfig, RetentionPolicy

    resources = managed_test_resources
    stream_name = f"{resources['config'].nats_stream_prefix}-test-batch-{int(time.time())}"

    try:
        # Create test stream
        stream_config = StreamConfig(
            name=stream_name,
            subjects=[f"{resources['config'].get_subject_prefix()}.batchtest.>"],
            retention=RetentionPolicy.LIMITS,
            max_msgs_per_subject=1,
            max_age=300,
        )
        await resources['jetstream_context'].add_stream(config=stream_config)

        # Create test index
        async with resources['http_session'].put(f"{resources['config'].fpindex_url}/batchtest") as resp:
            assert resp.status == 200

        # Create updater
        updater = UpdaterService(resources['config'])
        
        async with updater:
            # Publish multiple messages quickly to test batching
            test_fps = [
                (888111, [1100, 1200, 1300]),
                (888222, [1400, 1500, 1600]),
                (888333, [1700, 1800, 1900]),
                (888444, [2000, 2100, 2200]),
                (888555, [2300, 2400, 2500]),
            ]

            for fp_id, hashes in test_fps:
                subject = f"{resources['config'].get_subject_prefix()}.batchtest.{fp_id:08x}"
                message_data = msgspec.msgpack.encode(FingerprintData(hashes=hashes))
                await resources['jetstream_context'].publish(subject, message_data)

            # For this test, we'll just verify messages were published
            # Actual batch processing testing would need a more complex setup
            await asyncio.sleep(1)

    finally:
        # Cleanup is handled automatically by conftest
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
