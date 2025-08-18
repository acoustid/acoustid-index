#!/usr/bin/env python3

import aiohttp
import nats
from fpindexcluster.manager import IndexManager, IndexUpdater


def test_import_index_manager() -> None:
    """Test that IndexManager can be imported."""
    assert IndexManager is not None


def test_import_index_updater() -> None:
    """Test that IndexUpdater can be imported."""
    assert IndexUpdater is not None


async def test_instantiate_index_manager(nats_connection: nats.NATS) -> None:
    """Test that IndexManager can be instantiated."""
    manager = await IndexManager.create(
        nats_connection=nats_connection,
        stream_prefix="test",
        fpindex_url="http://localhost:6081",
        instance_name="test-instance",
    )

    assert manager is not None
    assert manager.stream_prefix == "test"
    assert manager.fpindex_url == "http://localhost:6081"
    assert manager.instance_name == "test-instance"

    # Clean up
    await manager.cleanup()


async def test_instantiate_index_updater(nats_connection: nats.NATS) -> None:
    """Test that IndexUpdater can be instantiated."""
    js = nats_connection.jetstream()

    async with aiohttp.ClientSession() as session:
        updater = IndexUpdater(
            index_name="test-index",
            js=js,
            http_session=session,
            stream_name="test_stream",
            subject="test.subject",
            fpindex_url="http://localhost:6081",
            instance_name="test-instance",
        )

        assert updater is not None
        assert updater.index_name == "test-index"
        assert updater.stream_name == "test_stream"
        assert updater.subject == "test.subject"
        assert updater.fpindex_url == "http://localhost:6081"
        assert updater.instance_name == "test-instance"
