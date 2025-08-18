#!/usr/bin/env python3

import logging
from aiohttp import web
from aiohttp.web import Response, json_response, Application, AppRunner, TCPSite
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


async def create_index(request):
    """Create a new index."""
    index_name = request.match_info["index"]
    manager = request.app["index_manager"]

    try:
        success, message, current_state = await manager.publish_create_index(index_name)

        if success:
            if current_state.value in [
                "active",
                "not_exists",
            ]:  # Newly created or was deleted
                logger.info(f"Triggered index creation for '{index_name}'")
                return json_response(
                    {
                        "status": "accepted",
                        "index": index_name,
                        "message": message,
                        "state": current_state.value,
                    },
                    status=202,
                )
            else:  # Already existed (idempotent)
                logger.info(f"Index '{index_name}' already exists")
                return json_response(
                    {
                        "status": "ok",
                        "index": index_name,
                        "message": message,
                        "state": current_state.value,
                    },
                    status=200,
                )
        else:
            # Invalid state transition
            logger.warning(f"Cannot create index '{index_name}': {message}")
            return json_response(
                {
                    "error": message,
                    "index": index_name,
                    "current_state": current_state.value,
                },
                status=409,
            )

    except Exception as e:
        logger.error(f"Error creating index '{index_name}': {e}")
        return json_response({"error": str(e)}, status=500)


async def delete_index(request):
    """Delete an index."""
    index_name = request.match_info["index"]
    manager = request.app["index_manager"]

    try:
        success, message, current_state = await manager.publish_delete_index(index_name)

        if success:
            if current_state.value in [
                "deleted",
                "not_exists",
            ]:  # Newly deleted or didn't exist
                if current_state.value == "deleted":
                    logger.info(f"Triggered index deletion for '{index_name}'")
                    return json_response(
                        {
                            "status": "accepted",
                            "index": index_name,
                            "message": message,
                            "state": current_state.value,
                        },
                        status=202,
                    )
                else:  # Didn't exist (idempotent)
                    logger.info(f"Index '{index_name}' does not exist")
                    return json_response(
                        {
                            "status": "ok",
                            "index": index_name,
                            "message": message,
                            "state": current_state.value,
                        },
                        status=200,
                    )
        else:
            # Invalid state transition
            logger.warning(f"Cannot delete index '{index_name}': {message}")
            return json_response(
                {
                    "error": message,
                    "index": index_name,
                    "current_state": current_state.value,
                },
                status=409,
            )

    except Exception as e:
        logger.error(f"Error deleting index '{index_name}': {e}")
        return json_response({"error": str(e)}, status=500)


async def get_index_status(request):
    """Get the current status of an index."""
    index_name = request.match_info["index"]
    manager = request.app["index_manager"]

    try:
        current_state = await manager.get_index_state(index_name)

        return json_response(
            {
                "index": index_name,
                "state": current_state.value,
                "exists": current_state.exists(),
            },
            status=200,
        )

    except Exception as e:
        logger.error(f"Error getting status for index '{index_name}': {e}")
        return json_response({"error": str(e)}, status=500)


def create_app(nats_connection: nats.NATS, index_manager) -> Application:
    """Create and configure the aiohttp application."""
    app = Application()

    # Store dependencies in app context
    app["nats_connection"] = nats_connection
    app["index_manager"] = index_manager

    # Add health check route
    app.router.add_get("/_health", health_check)

    # Add index management routes
    app.router.add_put("/{index}", create_index)
    app.router.add_delete("/{index}", delete_index)
    app.router.add_get("/{index}", get_index_status)

    logger.info("HTTP server configured with /_health and index management endpoints")
    return app


async def start_server(
    nats_connection: nats.NATS, index_manager, host: str = "0.0.0.0", port: int = 8081
) -> AppRunner:
    """Start the HTTP server and return the runner."""
    app = create_app(nats_connection, index_manager)
    runner = AppRunner(app)
    await runner.setup()

    site = TCPSite(runner, host, port)
    await site.start()

    logger.info(f"HTTP server started on http://{host}:{port}")
    return runner
