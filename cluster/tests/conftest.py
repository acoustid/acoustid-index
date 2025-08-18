#!/usr/bin/env python3

import pytest
import nats
import os


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