#!/usr/bin/env python3

import logging
from aiohttp import web
from aiohttp.web import Response
import nats


logger = logging.getLogger(__name__)


async def health_check(request):
    """Health check endpoint that returns OK."""
    # Access NATS connection from app context
    nc = request.app.get("nats_connection")
    if nc and nc.is_connected:
        return Response(text="OK", status=200)
    else:
        return Response(text="NATS disconnected", status=503)


def create_app(nats_connection: nats.NATS) -> web.Application:
    """Create and configure the aiohttp application."""
    app = web.Application()
    
    # Store NATS connection in app context
    app["nats_connection"] = nats_connection

    # Add health check route
    app.router.add_get("/_health", health_check)

    logger.info("HTTP server configured with /_health endpoint")
    return app


async def start_server(nats_connection: nats.NATS, host: str = "0.0.0.0", port: int = 8081) -> web.AppRunner:
    """Start the HTTP server and return the runner."""
    app = create_app(nats_connection)
    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, host, port)
    await site.start()

    logger.info(f"HTTP server started on http://{host}:{port}")
    return runner
