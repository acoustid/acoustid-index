"""HTTP proxy service that forwards requests to NATS JetStream"""

import asyncio
import logging
import time
from contextlib import AsyncExitStack
from typing import Optional, Any

import msgspec.msgpack
import nats
from aiohttp import web, ClientSession
from nats.js import JetStreamContext
from nats.js.api import StreamConfig, RetentionPolicy, StorageType

from .config import Config
from .models import (
    FingerprintData,
    SearchRequest,
    BulkUpdateRequest,
    PutFingerprintRequest,
    ErrorResponse,
    EmptyResponse,
    BulkUpdateResponse,
    BulkUpdateResult,
    IndexCreatedEvent,
    IndexDeletedEvent,
)


logger = logging.getLogger(__name__)


class ProxyService:
    """HTTP proxy service for fpindex clustering"""

    def __init__(self, config: Config):
        self.config = config
        self.app = web.Application()
        self._setup_routes()
        self._exit_stack: Optional[AsyncExitStack] = None
        
        # These will be set during async context manager entry
        self.nc: Any  # nats.NATS connection
        self.js: JetStreamContext
        self.http_session: ClientSession
        self.runner: web.AppRunner
        self.site: web.TCPSite

    def _setup_routes(self):
        """Setup HTTP routes that mirror fpindex API"""
        # Health endpoints
        self.app.router.add_get("/_health", self.health)
        self.app.router.add_get("/{index}/_health", self.index_health)

        # Bulk operations (must come before single fingerprint routes)
        self.app.router.add_post("/{index}/_update", self.bulk_update)
        self.app.router.add_post("/{index}/_search", self.search)

        # Index management
        self.app.router.add_put("/{index}", self.create_index)
        self.app.router.add_get("/{index}", self.get_index_info)
        self.app.router.add_delete("/{index}", self.delete_index)

        # Single fingerprint operations
        self.app.router.add_put("/{index}/{fp_id}", self.put_fingerprint)
        self.app.router.add_get("/{index}/{fp_id}", self.get_fingerprint)
        # HEAD is automatically handled by aiohttp for GET routes
        self.app.router.add_delete("/{index}/{fp_id}", self.delete_fingerprint)

    async def __aenter__(self):
        """Async context manager entry - initialize resources"""
        logger.info(
            f"Initializing proxy service on {self.config.proxy_host}:{self.config.proxy_port}"
        )
        
        self._exit_stack = AsyncExitStack()
        
        # Create HTTP session for forwarding requests
        self.http_session = await self._exit_stack.enter_async_context(
            ClientSession()
        )
        
        # Connect to NATS
        self.nc = await nats.connect(self.config.nats_url)
        # Register NATS connection for cleanup
        self._exit_stack.callback(self.nc.close)
        self.js = self.nc.jetstream()
        logger.info(f"Connected to NATS at {self.config.nats_url}")
        
        # Ensure control stream exists
        await self._ensure_control_stream()
        
        # Setup HTTP server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self._exit_stack.callback(self.runner.cleanup)
        
        self.site = web.TCPSite(self.runner, self.config.proxy_host, self.config.proxy_port)
        
        logger.info("Proxy service resources initialized")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup resources"""
        # Stop HTTP server
        if hasattr(self, 'site'):
            await self.site.stop()
        
        # Close all resources via exit stack
        if self._exit_stack:
            await self._exit_stack.aclose()
        
        logger.info("Proxy service stopped")
    
    async def run(self):
        """Run the proxy service - must be called within async context manager"""
        if not hasattr(self, 'nc') or not hasattr(self, 'site'):
            raise RuntimeError("ProxyService.run() must be called within async context manager")
        
        logger.info("Starting proxy service...")
        
        # Start HTTP server
        await self.site.start()
        logger.info(f"Proxy service running on {self.config.proxy_host}:{self.config.proxy_port}")
        
        # Keep running until context manager exits
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Proxy service shutting down...")
            raise


    async def _ensure_control_stream(self):
        """Ensure control stream exists for index lifecycle events"""
        stream_config = StreamConfig(
            name=self.config.get_stream_name("_ctrl"),
            subjects=["fpindex._ctrl.>"],
            retention=RetentionPolicy.LIMITS,
            max_age=7*24*3600,  # Keep events for 7 days
            storage=StorageType.FILE,
        )
        
        try:
            await self.js.add_stream(stream_config)
            logger.info(f"Control stream {self.config.get_stream_name('_ctrl')} created/verified")
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise

    async def _ensure_index_stream(self, index_name: str):
        """Ensure stream exists for specific index"""
        stream_name = self.config.get_stream_name(index_name)
        stream_config = StreamConfig(
            name=stream_name,
            subjects=[f"fpindex.{index_name}.>"],
            retention=RetentionPolicy.LIMITS,
            max_msgs_per_subject=1,
            storage=StorageType.FILE,
        )
        
        try:
            await self.js.add_stream(stream_config)
            logger.info(f"Stream {stream_name} created/verified for index {index_name}")
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise

    async def _publish_control_event(self, event_type: str, index_name: str, event_data):
        """Publish control event to management stream"""
        subject = f"fpindex._ctrl.index.{event_type}.{index_name}"
        message = msgspec.msgpack.encode(event_data)
        
        try:
            await self.js.publish(subject, message)
            logger.info(f"Published control event: {subject}")
        except Exception as e:
            logger.error(f"Failed to publish control event {subject}: {e}")
            # Don't fail the main operation if control event fails

    async def _publish_to_nats(
        self, index_name: str, fp_id: int, hashes: list[int] | None
    ) -> None:
        """Publish message to NATS JetStream"""
        # Ensure stream exists for this index
        await self._ensure_index_stream(index_name)
        
        subject = f"fpindex.{index_name}.{fp_id:08x}"

        if hashes is None:
            operation = "delete"
            message = b""
        else:
            operation = "insert"
            message = msgspec.msgpack.encode(FingerprintData(hashes=hashes))

        assert self.js is not None, "JetStream context is not initialized"
        try:
            await self.js.publish(subject, message)
            logger.debug(f"Published to {subject}: {operation}")
        except Exception as e:
            logger.error(f"Failed to publish to NATS: {e}")
            raise

    def _msgpack_response(
        self, data: msgspec.Struct, status: int = 200
    ) -> web.Response:
        """Create a MessagePack response from a msgspec.Struct"""
        response_data = msgspec.msgpack.encode(data)
        return web.Response(
            body=response_data, status=status, content_type="application/vnd.msgpack"
        )

    def _error_response(self, error_msg: str, status: int = 400) -> web.Response:
        """Create an error response"""
        return self._msgpack_response(ErrorResponse(error=error_msg), status)

    # Health endpoints
    async def health(self, request: web.Request) -> web.Response:
        """Global health check"""
        # Health endpoints return plain text "OK\n" in fpindex
        return web.Response(text="OK\n", content_type="text/plain")

    async def index_health(self, request: web.Request) -> web.Response:
        """Index-specific health check"""
        # Index health also returns plain text "OK\n" in fpindex
        return web.Response(text="OK\n", content_type="text/plain")

    # Index management
    async def create_index(self, request: web.Request) -> web.Response:
        """Create an index"""
        index_name = request.match_info["index"]
        
        # Create the index stream
        await self._ensure_index_stream(index_name)
        
        # Publish creation event
        event = IndexCreatedEvent(
            index_name=index_name,
            created_at=int(time.time()),
            stream_name=self.config.get_stream_name(index_name)
        )
        await self._publish_control_event("created", index_name, event)
        
        return self._msgpack_response(EmptyResponse())

    async def get_index_info(self, request: web.Request) -> web.Response:
        """Get index information - this needs to query the actual fpindex"""
        # This should forward to actual fpindex instance
        # For now, return placeholder - fpindex returns GetIndexResponse with version/segments/docs/attributes
        # TODO: Forward this to actual fpindex instance
        return self._error_response("NotImplemented", 501)

    async def delete_index(self, request: web.Request) -> web.Response:
        """Delete an index and all its data"""
        index_name = request.match_info["index"]
        stream_name = self.config.get_stream_name(index_name)
        
        try:
            # Delete the entire stream = delete all fingerprints in index
            await self.js.delete_stream(stream_name)
            logger.info(f"Deleted stream {stream_name} for index {index_name}")
            
            # Publish deletion event  
            event = IndexDeletedEvent(
                index_name=index_name,
                deleted_at=int(time.time()),
                stream_name=stream_name
            )
            await self._publish_control_event("deleted", index_name, event)
            
            return self._msgpack_response(EmptyResponse())
            
        except Exception as e:
            logger.error(f"Failed to delete index stream: {e}")
            return self._error_response(f"Failed to delete index: {e}", 500)

    # Single fingerprint operations
    def _validate_fingerprint_id(self, fp_id_str: str) -> tuple[bool, int]:
        """Validate that fingerprint ID is a valid 32-bit unsigned integer"""
        try:
            fp_id = int(fp_id_str)
            if 0 <= fp_id <= 0xFFFFFFFF:  # 32-bit unsigned integer range
                return True, fp_id
            return False, 0
        except ValueError:
            return False, 0

    async def put_fingerprint(self, request: web.Request) -> web.Response:
        """Insert/update a single fingerprint"""
        index_name = request.match_info["index"]
        fp_id_str = request.match_info["fp_id"]

        # Validate fingerprint ID is 32-bit unsigned integer
        is_valid, fp_id = self._validate_fingerprint_id(fp_id_str)
        if not is_valid:
            return self._error_response(
                "Fingerprint ID must be a 32-bit unsigned integer"
            )

        try:
            body = await request.read()
            request_data = msgspec.msgpack.decode(body, type=PutFingerprintRequest)
        except Exception:
            return self._error_response("Invalid MessagePack")

        # Publish to NATS using hex-encoded fingerprint ID as subject
        await self._publish_to_nats(index_name, fp_id, request_data.hashes)

        # fpindex returns EmptyResponse for PUT fingerprint operations
        return self._msgpack_response(EmptyResponse())

    async def get_fingerprint(self, request: web.Request) -> web.Response:
        """Get fingerprint info - this needs to query actual fpindex"""
        # This should forward to actual fpindex instance
        # fpindex returns GetFingerprintResponse with version field or 404 error
        # TODO: Forward this to actual fpindex instance
        return self._error_response("NotImplemented", 501)

    async def delete_fingerprint(self, request: web.Request) -> web.Response:
        """Delete a fingerprint"""
        index_name = request.match_info["index"]
        fp_id_str = request.match_info["fp_id"]

        # Validate fingerprint ID is 32-bit unsigned integer
        is_valid, fp_id = self._validate_fingerprint_id(fp_id_str)
        if not is_valid:
            return self._error_response(
                "Fingerprint ID must be a 32-bit unsigned integer"
            )

        # Delete publishes empty message using hex-encoded ID
        await self._publish_to_nats(index_name, fp_id, None)

        # fpindex returns EmptyResponse for DELETE fingerprint operations
        return self._msgpack_response(EmptyResponse())

    # Bulk operations
    async def bulk_update(self, request: web.Request) -> web.Response:
        """Handle bulk update operations"""
        index_name = request.match_info["index"]

        try:
            body = await request.read()
            request_data = msgspec.msgpack.decode(body, type=BulkUpdateRequest)
        except Exception:
            return self._error_response("Invalid MessagePack")

        # Process each change and publish to NATS
        results = []
        for change in request_data.changes:
            try:
                if change.insert:
                    insert_data = change.insert
                    await self._publish_to_nats(
                        index_name, insert_data.id, insert_data.hashes
                    )
                    results.append(
                        BulkUpdateResult(id=insert_data.id, status="inserted")
                    )

                elif change.update:
                    update_data = change.update
                    await self._publish_to_nats(
                        index_name, update_data.id, update_data.hashes
                    )
                    results.append(
                        BulkUpdateResult(id=update_data.id, status="updated")
                    )

                elif change.delete:
                    delete_data = change.delete
                    # Delete sends empty message using hex-encoded ID
                    await self._publish_to_nats(index_name, delete_data.id, None)
                    results.append(
                        BulkUpdateResult(id=delete_data.id, status="deleted")
                    )

            except Exception as e:
                logger.error(f"Failed to process change {change}: {e}")
                results.append(BulkUpdateResult(error=str(e)))

        return self._msgpack_response(BulkUpdateResponse(results=results))

    async def search(self, request: web.Request) -> web.Response:
        """Handle search requests - forward to actual fpindex"""
        index_name = request.match_info["index"]

        try:
            body = await request.read()
            search_request = msgspec.msgpack.decode(body, type=SearchRequest)
        except Exception:
            return self._error_response("Invalid MessagePack")

        # For search, we need to forward to an actual fpindex instance
        # For now, forward to the configured fpindex_url
        try:
            # Forward as MessagePack to fpindex
            request_data = msgspec.msgpack.encode(search_request)
            assert self.http_session is not None
            async with self.http_session.post(
                f"{self.config.fpindex_url}/{index_name}/_search",
                data=request_data,
                headers={
                    "Content-Type": "application/vnd.msgpack",
                    "Accept": "application/vnd.msgpack",
                },
            ) as resp:
                result_body = await resp.read()
                return web.Response(
                    body=result_body,
                    status=resp.status,
                    content_type="application/vnd.msgpack",
                )
        except Exception as e:
            logger.error(f"Failed to forward search request: {e}")
            return self._error_response("Search service unavailable", 503)
