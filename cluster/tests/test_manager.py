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


async def test_instantiate_index_manager(nats_connection: nats.NATS, fpindex_url: str) -> None:
    """Test that IndexManager can be instantiated."""
    manager = await IndexManager.create(
        nats_connection=nats_connection,
        stream_prefix="test",
        fpindex_url=fpindex_url,
        instance_name="test-instance",
    )

    assert manager is not None
    assert manager.stream_prefix == "test"
    assert manager.fpindex_url == fpindex_url
    assert manager.instance_name == "test-instance"

    # Clean up
    await manager.cleanup()


async def test_instantiate_index_updater(nats_connection: nats.NATS, fpindex_url: str) -> None:
    """Test that IndexUpdater can be instantiated."""
    js = nats_connection.jetstream()

    async with aiohttp.ClientSession() as session:
        updater = IndexUpdater(
            index_name="test-index",
            js=js,
            http_session=session,
            stream_name="test_stream",
            subject="test.subject",
            fpindex_url=fpindex_url,
            instance_name="test-instance",
            manager=None,
        )

        assert updater is not None
        assert updater.index_name == "test-index"
        assert updater.stream_name == "test_stream"
        assert updater.subject == "test.subject"
        assert updater.fpindex_url == fpindex_url
        assert updater.instance_name == "test-instance"


async def test_fpindex_server_health(fpindex_url: str) -> None:
    """Test that the fpindex server fixture is working."""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{fpindex_url}/_health") as resp:
            assert resp.status == 200
            text = await resp.text()
            assert text.strip() == "OK"
