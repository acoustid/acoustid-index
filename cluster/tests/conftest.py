#!/usr/bin/env python3

import pytest
import nats
import os
import asyncio
import asyncio.subprocess
import socket
import aiohttp
from contextlib import closing
from pathlib import Path


def find_free_port():
    """Find a free port to bind the server to."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


class ServerNotReady(Exception):
    pass


class ServerDied(Exception):
    pass


class FpIndexServerManager:
    """Manages a real fpindex server instance for testing."""

    def __init__(self, base_dir, port):
        self.data_dir = base_dir / "data"
        self.log_file = base_dir / "server.log"
        self.port = port
        self.process = None

    async def start(self):
        # Find the fpindex binary
        binary_path = Path(__file__).parent.parent.parent / "zig-out" / "bin" / "fpindex"
        if not binary_path.exists():
            raise RuntimeError(f"fpindex binary not found at {binary_path}. Please run 'zig build' first.")

        command = [
            str(binary_path),
            "--dir",
            str(self.data_dir),
            "--port",
            str(self.port),
            "--log-level",
            "debug",
            "--parallel-loading-threshold",
            "2",
        ]

        with self.log_file.open("w") as log_f:
            self.process = await asyncio.create_subprocess_exec(
                *command, stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL, stderr=log_f
            )

        await self.wait_for_ready()

    async def stop(self, kill=False):
        if self.process is not None:
            if self.process.returncode is None:
                if kill:
                    self.process.kill()
                else:
                    self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    self.process.kill()
                    await self.process.wait()

    async def wait_for_ready(self, timeout=10.0):
        start_time = asyncio.get_event_loop().time()
        deadline = start_time + timeout

        while asyncio.get_event_loop().time() < deadline:
            url = f"http://localhost:{self.port}/_health"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=1)) as resp:
                        resp.raise_for_status()
                        return
            except Exception:
                # Check if process died
                if self.process.returncode is not None:
                    self.print_error_log()
                    raise ServerDied()

                # Wait a bit before retrying
                await asyncio.sleep(0.1)

        self.print_error_log()
        raise ServerNotReady()

    def print_error_log(self):
        if self.log_file.exists():
            for line in self.log_file.read_text().splitlines():
                print(line)

    @property
    def url(self):
        return f"http://localhost:{self.port}"


@pytest.fixture
async def nats_connection():
    """Provide a NATS connection for testing."""
    # Use environment variable or default to localhost
    nats_url = os.getenv("TEST_NATS_URL", "nats://localhost:4222")

    nc = None
    try:
        nc = await nats.connect(nats_url)
        yield nc
    except Exception:
        # If NATS server is not available, skip tests that require it
        pytest.skip("NATS server not available")
    finally:
        if nc:
            await nc.close()


@pytest.fixture(scope="session")
async def fpindex_server(tmp_path_factory):
    """Start a real fpindex server on a random port and provide the server manager."""
    port = find_free_port()
    srv = FpIndexServerManager(base_dir=tmp_path_factory.mktemp("fpindex"), port=port)
    await srv.start()
    try:
        yield srv
    finally:
        await srv.stop()
        srv.print_error_log()


@pytest.fixture(scope="session")
def fpindex_url(fpindex_server):
    """Provide the URL of the running fpindex server."""
    return fpindex_server.url
