#!/usr/bin/env python3

import logging
import re
from aiohttp.web import Response, json_response, Application, AppRunner, TCPSite, Request
import nats

from .errors import InconsistentIndexState
from .models import Change, Insert, Delete


logger = logging.getLogger(__name__)

# Index name validation regex: alphanumeric start, then alphanumeric/underscore/hyphen
INDEX_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]*$")


def is_valid_index_name(name: str) -> bool:
    """
    Validate index name according to fpindex rules.

    Rules:
    - First character must be alphanumeric (0-9, A-Z, a-z)
    - Subsequent characters can be alphanumeric plus underscore (_) and hyphen (-)
    - No other characters allowed
    """
    return bool(name and INDEX_NAME_PATTERN.match(name))


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

    # Validate index name
    if not is_valid_index_name(index_name):
        logger.warning(f"Invalid index name: '{index_name}'")
        return json_response(
            {
                "error": "Invalid index name. Must start with alphanumeric character and contain only alphanumeric, underscore, and hyphen characters.",
                "index": index_name,
            },
            status=400,
        )

    try:
        await manager.publish_create_index(index_name)
    except InconsistentIndexState as exc:
        logger.warning("Failed to create index %s", index_name, exc_info=True)
        return json_response({"error": str(exc)}, status=409)
    except Exception as exc:
        logger.error("Failed to create index %s", index_name, exc_info=True)
        return json_response({"error": str(exc)}, status=500)

    return json_response({}, status=200)


async def delete_index(request):
    """Delete an index."""
    index_name = request.match_info["index"]
    manager = request.app["index_manager"]

    # Validate index name
    if not is_valid_index_name(index_name):
        logger.warning(f"Invalid index name: '{index_name}'")
        return json_response(
            {
                "error": "Invalid index name. Must start with alphanumeric character and contain only alphanumeric, underscore, and hyphen characters.",
                "index": index_name,
            },
            status=400,
        )

    try:
        await manager.publish_delete_index(index_name)
    except InconsistentIndexState as exc:
        logger.warning("Failed to delete index %s", index_name, exc_info=True)
        return json_response({"error": str(exc)}, status=409)
    except Exception as exc:
        logger.error("Failed to delete index %s", index_name, exc_info=True)
        return json_response({"error": str(exc)}, status=500)

    return json_response({}, status=200)


async def get_index_status(request):
    """Get the current status of an index."""
    index_name = request.match_info["index"]
    manager = request.app["index_manager"]

    # Validate index name
    if not is_valid_index_name(index_name):
        logger.warning(f"Invalid index name: '{index_name}'")
        return json_response(
            {
                "error": "Invalid index name. Must start with alphanumeric character and contain only alphanumeric, underscore, and hyphen characters.",
                "index": index_name,
            },
            status=400,
        )

    try:
        current_status = await manager.get_index_status(index_name)
    except Exception as e:
        logger.error(f"Error getting status for index '{index_name}': {e}")
        return json_response({"error": str(e)}, status=500)

    if current_status.status.active:
        return json_response({}, status=200)
    else:
        return json_response({}, status=404)


async def update_index(request: Request):
    """Handle fingerprint updates - POST /{index}/_update"""
    index_name = request.match_info["index"]
    manager = request.app["index_manager"]

    # Validate index name
    if not is_valid_index_name(index_name):
        logger.warning(f"Invalid index name: '{index_name}'")
        return json_response(
            {
                "error": "Invalid index name. Must start with alphanumeric character and contain only alphanumeric, underscore, and hyphen characters.",
                "index": index_name,
            },
            status=400,
        )

    try:
        # Parse request body as JSON (following fpindex UpdateRequestJSON structure)
        request_data = await request.json()
        
        # Validate required fields
        if "changes" not in request_data:
            return json_response({"error": "Missing required field: changes"}, status=400)
        
        # Convert request to our Change objects
        changes = []
        for change_data in request_data["changes"]:
            if "insert" in change_data and change_data["insert"] is not None:
                insert_data = change_data["insert"]
                if "id" not in insert_data or "hashes" not in insert_data:
                    return json_response({"error": "Invalid insert operation: missing id or hashes"}, status=400)
                changes.append(Change(
                    insert=Insert(id=insert_data["id"], hashes=insert_data["hashes"]),
                    delete=None
                ))
            elif "delete" in change_data and change_data["delete"] is not None:
                delete_data = change_data["delete"]
                if "id" not in delete_data:
                    return json_response({"error": "Invalid delete operation: missing id"}, status=400)
                changes.append(Change(
                    insert=None,
                    delete=Delete(id=delete_data["id"])
                ))
            else:
                return json_response({"error": "Each change must have either insert or delete operation"}, status=400)
        
        if not changes:
            return json_response({"error": "No changes provided"}, status=400)
        
        # Extract optional metadata
        metadata = request_data.get("metadata")
        
        # Publish update to NATS
        await manager.publish_update(index_name, changes, metadata)
        
        # Return success (no version like fpindex since we don't do version locking)
        return json_response({}, status=200)
        
    except ValueError as e:
        logger.warning(f"Invalid JSON in update request for '{index_name}': {e}")
        return json_response({"error": "Invalid JSON format"}, status=400)
    except InconsistentIndexState as exc:
        logger.warning("Failed to update index %s", index_name, exc_info=True)
        return json_response({"error": str(exc)}, status=409)
    except Exception as exc:
        logger.error("Failed to update index %s", index_name, exc_info=True)
        return json_response({"error": str(exc)}, status=500)


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
    
    # Add update route
    app.router.add_post("/{index}/_update", update_index)

    logger.info("HTTP server configured with /_health, index management, and update endpoints")
    return app


async def start_server(nats_connection: nats.NATS, index_manager, host: str = "0.0.0.0", port: int = 8081) -> AppRunner:
    """Start the HTTP server and return the runner."""
    app = create_app(nats_connection, index_manager)
    runner = AppRunner(app)
    await runner.setup()

    site = TCPSite(runner, host, port)
    await site.start()

    logger.info(f"HTTP server started on http://{host}:{port}")
    return runner
