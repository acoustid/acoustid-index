"""HTTP proxy service that forwards requests to NATS JetStream"""

import logging
from typing import Optional

import msgspec.msgpack
import nats
from aiohttp import web, ClientSession
from nats.js import JetStreamContext

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
)


logger = logging.getLogger(__name__)


class ProxyService:
    """HTTP proxy service for fpindex clustering"""
    
    def __init__(self, config: Config):
        self.config = config
        self.nc: Optional[nats.NATS] = None
        self.js: Optional[JetStreamContext] = None
        self.http_session: Optional[ClientSession] = None
        self.app = web.Application()
        self._setup_routes()
    
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
    
    async def start(self):
        """Start the proxy service"""
        logger.info(f"Starting proxy service on {self.config.proxy_host}:{self.config.proxy_port}")
        
        # Create HTTP session for forwarding requests
        self.http_session = ClientSession()
        
        # Connect to NATS
        self.nc = await nats.connect(self.config.nats_url)
        self.js = self.nc.jetstream()
        logger.info(f"Connected to NATS at {self.config.nats_url}")
        
        # Start HTTP server
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.config.proxy_host, self.config.proxy_port)
        await site.start()
        logger.info("Proxy service started")
    
    async def stop(self):
        """Stop the proxy service"""
        if self.http_session:
            await self.http_session.close()
        if self.nc:
            await self.nc.close()
        logger.info("Proxy service stopped")
    
    async def _publish_to_nats(self, index_name: str, fp_id: int, hashes: list[int] | None) -> None:
        """Publish message to NATS JetStream"""
        subject = f"{self.config.nats_stream}.{index_name}.{fp_id:08x}"

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
        # No need to publish anything for index creation - 
        # indexes are created implicitly when first fingerprint is added
        # fpindex returns EmptyResponse for PUT operations
        response = EmptyResponse()
        response_data = msgspec.msgpack.encode(response)
        return web.Response(body=response_data, content_type="application/vnd.msgpack")
    
    async def get_index_info(self, request: web.Request) -> web.Response:
        """Get index information - this needs to query the actual fpindex"""
        # This should forward to actual fpindex instance 
        # For now, return placeholder - fpindex returns GetIndexResponse with version/segments/docs/attributes
        # TODO: Forward this to actual fpindex instance
        error_response = ErrorResponse(error="NotImplemented")
        response_data = msgspec.msgpack.encode(error_response)
        return web.Response(body=response_data, status=501, content_type="application/vnd.msgpack")
    
    async def delete_index(self, request: web.Request) -> web.Response:
        """Delete an index"""
        # Index deletion would require purging all messages for the index
        # This is a more complex operation - fpindex returns EmptyResponse for DELETE operations
        response = EmptyResponse()
        response_data = msgspec.msgpack.encode(response)
        return web.Response(body=response_data, content_type="application/vnd.msgpack")
    
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
            error_response = ErrorResponse(error="Fingerprint ID must be a 32-bit unsigned integer")
            response_data = msgspec.msgpack.encode(error_response)
            return web.Response(body=response_data, status=400, content_type="application/vnd.msgpack")

        try:
            body = await request.read()
            request_data = msgspec.msgpack.decode(
                body, type=PutFingerprintRequest
            )
        except Exception:
            error_response = ErrorResponse(error="Invalid MessagePack")
            response_data = msgspec.msgpack.encode(error_response)
            return web.Response(
                body=response_data,
                status=400,
                content_type="application/vnd.msgpack"
            )

        # Publish to NATS using hex-encoded fingerprint ID as subject
        await self._publish_to_nats(index_name, fp_id, request_data.hashes)

        # fpindex returns EmptyResponse for PUT fingerprint operations
        response = EmptyResponse()
        response_data = msgspec.msgpack.encode(response)
        return web.Response(body=response_data, content_type="application/vnd.msgpack")

    async def get_fingerprint(self, request: web.Request) -> web.Response:
        """Get fingerprint info - this needs to query actual fpindex"""
        # This should forward to actual fpindex instance
        # fpindex returns GetFingerprintResponse with version field or 404 error
        # TODO: Forward this to actual fpindex instance  
        error_response = ErrorResponse(error="NotImplemented")
        response_data = msgspec.msgpack.encode(error_response)
        return web.Response(body=response_data, status=501, content_type="application/vnd.msgpack")

    async def delete_fingerprint(self, request: web.Request) -> web.Response:
        """Delete a fingerprint"""
        index_name = request.match_info["index"]
        fp_id_str = request.match_info["fp_id"]
        
        # Validate fingerprint ID is 32-bit unsigned integer
        is_valid, fp_id = self._validate_fingerprint_id(fp_id_str)
        if not is_valid:
            error_response = ErrorResponse(error="Fingerprint ID must be a 32-bit unsigned integer")
            response_data = msgspec.msgpack.encode(error_response)
            return web.Response(body=response_data, status=400, content_type="application/vnd.msgpack")
        
        # Delete publishes empty message using hex-encoded ID
        await self._publish_to_nats(index_name, fp_id, None)
        
        # fpindex returns EmptyResponse for DELETE fingerprint operations
        response = EmptyResponse()
        response_data = msgspec.msgpack.encode(response)
        return web.Response(body=response_data, content_type="application/vnd.msgpack")
    
    # Bulk operations
    async def bulk_update(self, request: web.Request) -> web.Response:
        """Handle bulk update operations"""
        index_name = request.match_info["index"]
        
        try:
            body = await request.read()
            request_data = msgspec.msgpack.decode(body, type=BulkUpdateRequest)
        except Exception:
            error_response = ErrorResponse(error="Invalid MessagePack")
            response_data = msgspec.msgpack.encode(error_response)
            return web.Response(body=response_data, status=400, content_type="application/vnd.msgpack")
        
        # Process each change and publish to NATS
        results = []
        for change in request_data.changes:
            try:
                if change.insert:
                    insert_data = change.insert
                    await self._publish_to_nats(index_name, insert_data.id, insert_data.hashes)
                    results.append(BulkUpdateResult(id=insert_data.id, status="inserted"))
                
                elif change.update:
                    update_data = change.update
                    await self._publish_to_nats(index_name, update_data.id, update_data.hashes)
                    results.append(BulkUpdateResult(id=update_data.id, status="updated"))
                
                elif change.delete:
                    delete_data = change.delete
                    # Delete sends empty message using hex-encoded ID
                    await self._publish_to_nats(index_name, delete_data.id, None)
                    results.append(BulkUpdateResult(id=delete_data.id, status="deleted"))
                
            except Exception as e:
                logger.error(f"Failed to process change {change}: {e}")
                results.append(BulkUpdateResult(error=str(e)))
        
        response = BulkUpdateResponse(results=results)
        response_data = msgspec.msgpack.encode(response)
        return web.Response(body=response_data, content_type="application/vnd.msgpack")
    
    async def search(self, request: web.Request) -> web.Response:
        """Handle search requests - forward to actual fpindex"""
        index_name = request.match_info["index"]
        
        try:
            body = await request.read()
            search_request = msgspec.msgpack.decode(body, type=SearchRequest)
        except Exception:
            error_response = ErrorResponse(error="Invalid MessagePack")
            response_data = msgspec.msgpack.encode(error_response)
            return web.Response(body=response_data, status=400, content_type="application/vnd.msgpack")
        
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
                    "Accept": "application/vnd.msgpack"
                }
            ) as resp:
                result_body = await resp.read()
                return web.Response(
                    body=result_body,
                    status=resp.status,
                    content_type="application/vnd.msgpack"
                )
        except Exception as e:
            logger.error(f"Failed to forward search request: {e}")
            error_response = ErrorResponse(error="Search service unavailable")
            response_data = msgspec.msgpack.encode(error_response)
            return web.Response(
                body=response_data,
                status=503,
                content_type="application/vnd.msgpack"
            )
