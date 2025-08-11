"""Test configuration and fixtures for fpindexcluster tests

This module provides:
- Automatic unique stream prefix generation for test isolation
- Automatic cleanup of NATS streams and fpindex indexes
- Shared fixtures for test services
"""

import os
import time
import uuid
import asyncio
from typing import AsyncGenerator, List, Tuple, Set
from contextlib import AsyncExitStack

import pytest
import nats
from aiohttp import ClientSession
from nats.js import JetStreamContext

from fpindexcluster.config import Config


class TestConfig(Config):
    """Test configuration with unique prefix generation"""
    
    def __init__(self):
        # Generate unique prefix for this test session
        timestamp = int(time.time())
        session_id = uuid.uuid4().hex[:8]
        unique_prefix = f"fpindex-test-{timestamp}-{session_id}"
        
        super().__init__(
            nats_url=os.getenv("TEST_NATS_URL", "nats://localhost:4222"),
            nats_stream_prefix=unique_prefix,
            fpindex_url=os.getenv("TEST_FPINDEX_URL", "http://localhost:6081"),
            consumer_name=f"test-consumer-{unique_prefix}",
            proxy_host="127.0.0.1",
            proxy_port=8080,
        )


class TestCleanupManager:
    """Manages cleanup of test resources"""
    
    def __init__(self, nats_client: nats.NATS, js: JetStreamContext, http_session: ClientSession, config: TestConfig):
        self.nc = nats_client
        self.js = js
        self.session = http_session
        self.config = config
    
    async def discover_all_test_resources(self) -> Tuple[Set[str], Set[str], Set[Tuple[str, str]]]:
        """Discover all resources with our test prefix"""
        streams = set()
        indexes = set()
        consumers = set()
        
        # Discover NATS streams with our prefix
        try:
            stream_infos = await self.js.streams_info()
            for stream in stream_infos:
                if stream.config.name.startswith(self.config.nats_stream_prefix):
                    streams.add(stream.config.name)
                    
                    # Discover consumers in this stream
                    try:
                        consumer_infos = await self.js.consumers_info(stream.config.name)
                        for consumer in consumer_infos:
                            if consumer.config.name and consumer.config.name.startswith("test-consumer"):
                                consumers.add((stream.config.name, consumer.config.name))
                    except Exception:
                        # Consumer listing might fail
                        pass
        except Exception:
            # NATS might not be available
            pass
        
        # Discover fpindex indexes - try to get a list from fpindex
        try:
            # fpindex doesn't have a list endpoint, so we'll try common test index names
            # and any that exist will be cleaned up
            test_index_names = ["main", "test", "testindex", "batchtest"]
            for index_name in test_index_names:
                try:
                    async with self.session.get(f"{self.config.fpindex_url}/{index_name}") as resp:
                        if resp.status == 200:
                            indexes.add(index_name)
                except Exception:
                    pass
        except Exception:
            # fpindex might not be available
            pass
        
        return streams, indexes, consumers
    
    async def cleanup_all(self):
        """Clean up all resources with our test prefix"""
        streams, indexes, consumers = await self.discover_all_test_resources()
        
        # Clean up consumers first
        for stream_name, consumer_name in consumers:
            try:
                await self.js.delete_consumer(stream_name, consumer_name)
                print(f"Cleaned up consumer: {consumer_name} from stream: {stream_name}")
            except Exception:
                # Consumer might not exist or stream might be gone
                pass
        
        # Clean up NATS streams
        for stream_name in streams:
            try:
                await self.js.delete_stream(stream_name)
                print(f"Cleaned up NATS stream: {stream_name}")
            except Exception:
                # Stream might not exist
                pass
        
        # Clean up fpindex indexes
        for index_name in indexes:
            try:
                async with self.session.delete(f"{self.config.fpindex_url}/{index_name}") as resp:
                    if resp.status in [200, 204]:
                        print(f"Cleaned up fpindex index: {index_name}")
            except Exception:
                # Index might not exist or fpindex might be unavailable
                pass


@pytest.fixture(scope="session")
async def test_config() -> TestConfig:
    """Provide test configuration with unique prefix"""
    return TestConfig()


@pytest.fixture
async def nats_connection(test_config: TestConfig) -> AsyncGenerator[nats.NATS, None]:
    """Provide NATS connection for each test"""
    try:
        nc = await nats.connect(test_config.nats_url)
        yield nc
        await nc.close()
    except Exception:
        # NATS not available, skip tests that require it
        pytest.skip(f"NATS not available at {test_config.nats_url}")


@pytest.fixture
async def jetstream_context(nats_connection: nats.NATS) -> JetStreamContext:
    """Provide JetStream context for each test"""
    return nats_connection.jetstream()


@pytest.fixture
async def http_session() -> AsyncGenerator[ClientSession, None]:
    """Provide HTTP session for each test"""
    session = ClientSession()
    yield session
    await session.close()


@pytest.fixture
async def fpindex_availability(http_session: ClientSession, test_config: TestConfig) -> bool:
    """Check if fpindex is available"""
    try:
        async with http_session.get(f"{test_config.fpindex_url}/_health") as resp:
            return resp.status == 200
    except Exception:
        return False


@pytest.fixture
async def cleanup_manager(
    nats_connection: nats.NATS,
    jetstream_context: JetStreamContext,
    http_session: ClientSession,
    test_config: TestConfig
) -> AsyncGenerator[TestCleanupManager, None]:
    """Provide cleanup manager for each test"""
    manager = TestCleanupManager(nats_connection, jetstream_context, http_session, test_config)
    
    yield manager
    
    # Clean up all resources at the end of the test
    await manager.cleanup_all()


# Auto-cleanup fixtures are no longer needed - cleanup happens automatically


# Convenience fixtures that combine common needs

@pytest.fixture
async def managed_test_resources(
    test_config: TestConfig,
    nats_connection: nats.NATS,
    jetstream_context: JetStreamContext,
    http_session: ClientSession,
    cleanup_manager: TestCleanupManager,
):
    """Provide all managed test resources in one fixture"""
    return {
        'config': test_config,
        'nats_connection': nats_connection,
        'jetstream_context': jetstream_context,
        'http_session': http_session,
        'cleanup_manager': cleanup_manager,
    }


@pytest.fixture
def skip_if_no_nats(nats_connection):
    """Skip test if NATS is not available"""
    # If we get here, NATS connection succeeded
    pass


@pytest.fixture
def skip_if_no_fpindex(fpindex_availability):
    """Skip test if fpindex is not available"""
    if not fpindex_availability:
        pytest.skip("fpindex not available")