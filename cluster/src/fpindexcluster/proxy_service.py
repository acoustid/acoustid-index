"""HTTP proxy service that forwards requests to NATS JetStream"""

import json
import logging
from typing import Any, Dict, Optional

import msgspec.msgpack
import nats
from aiohttp import web, ClientSession
from nats.js import JetStreamContext

from .config import Config
from .models import FingerprintData


logger = logging.getLogger(__name__)


class ProxyService:
    """HTTP proxy service for fpindex clustering"""
    
    def __init__(self, config: Config):
        self.config = config
        self.nc: Optional[nats.NATS] = None
        self.js: Optional[JetStreamContext] = None
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
            logger.debug(f"Published to {subject}: {operation} -> {message}")
        except Exception as e:
            logger.error(f"Failed to publish to NATS: {e}")
            raise
    
    # Health endpoints
    async def health(self, request: web.Request) -> web.Response:
        """Global health check"""
        return web.json_response({"status": "ok"})
    
    async def index_health(self, request: web.Request) -> web.Response:
        """Index-specific health check"""
        index_name = request.match_info["index"]
        return web.json_response({"status": "ok", "index": index_name})
    
    # Index management
    async def create_index(self, request: web.Request) -> web.Response:
        """Create an index"""
        index_name = request.match_info["index"]
        
        # No need to publish anything for index creation - 
        # indexes are created implicitly when first fingerprint is added
        
        return web.json_response({"status": "created", "index": index_name})
    
    async def get_index_info(self, request: web.Request) -> web.Response:
        """Get index information - this needs to query the actual fpindex"""
        # For now, return a simple response
        # In practice, this might need to query one of the fpindex instances
        index_name = request.match_info["index"]
        return web.json_response({"index": index_name, "status": "active"})
    
    async def delete_index(self, request: web.Request) -> web.Response:
        """Delete an index"""
        index_name = request.match_info["index"]
        # Index deletion would require purging all messages for the index
        # This is a more complex operation - for now just return success
        return web.json_response({"status": "deleted", "index": index_name})
    
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
            return web.json_response({"error": "Fingerprint ID must be a 32-bit unsigned integer"}, status=400)

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        if "hashes" not in data:
            return web.json_response({"error": "Missing 'hashes' field"}, status=400)

        # Publish to NATS using hex-encoded fingerprint ID as subject
        await self._publish_to_nats(index_name, fp_id, data["hashes"])

        return web.json_response({"status": "ok", "id": fp_id})

    async def get_fingerprint(self, request: web.Request) -> web.Response:
        """Get fingerprint info - this needs to query actual fpindex"""
        # For now, return a placeholder
        # In practice, this might need to query one of the fpindex instances
        index_name = request.match_info["index"]
        fp_id = request.match_info["fp_id"]
        return web.json_response({"id": fp_id, "index": index_name})

    async def delete_fingerprint(self, request: web.Request) -> web.Response:
        """Delete a fingerprint"""
        index_name = request.match_info["index"]
        fp_id_str = request.match_info["fp_id"]
        
        # Validate fingerprint ID is 32-bit unsigned integer
        is_valid, fp_id = self._validate_fingerprint_id(fp_id_str)
        if not is_valid:
            return web.json_response({"error": "Fingerprint ID must be a 32-bit unsigned integer"}, status=400)
        
        # Delete publishes empty message using hex-encoded ID
        await self._publish_to_nats(index_name, fp_id, None)
        return web.json_response({"status": "deleted", "id": fp_id})
    
    # Bulk operations
    async def bulk_update(self, request: web.Request) -> web.Response:
        """Handle bulk update operations"""
        index_name = request.match_info["index"]
        
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        
        if "changes" not in data:
            return web.json_response({"error": "Missing 'changes' field"}, status=400)
        
        # Process each change and publish to NATS
        results = []
        for change in data["changes"]:
            try:
                if "insert" in change:
                    insert_data = change["insert"]
                    is_valid, fp_id = self._validate_fingerprint_id(str(insert_data["id"]))
                    if not is_valid:
                        results.append({"error": "Invalid fingerprint ID", "change": change})
                        continue
                    
                    await self._publish_to_nats(index_name, fp_id, insert_data["hashes"])
                    results.append({"id": fp_id, "status": "inserted"})
                
                elif "update" in change:
                    update_data = change["update"]
                    is_valid, fp_id = self._validate_fingerprint_id(str(update_data["id"]))
                    if not is_valid:
                        results.append({"error": "Invalid fingerprint ID", "change": change})
                        continue
                    
                    await self._publish_to_nats(index_name, fp_id, update_data["hashes"])
                    results.append({"id": fp_id, "status": "updated"})
                
                elif "delete" in change:
                    delete_data = change["delete"]
                    is_valid, fp_id = self._validate_fingerprint_id(str(delete_data["id"]))
                    if not is_valid:
                        results.append({"error": "Invalid fingerprint ID", "change": change})
                        continue
                    
                    # Delete sends empty message using hex-encoded ID
                    await self._publish_to_nats(index_name, fp_id, None)
                    results.append({"id": fp_id, "status": "deleted"})
                
            except Exception as e:
                logger.error(f"Failed to process change {change}: {e}")
                results.append({"error": str(e), "change": change})
        
        return web.json_response({"results": results})
    
    async def search(self, request: web.Request) -> web.Response:
        """Handle search requests - forward to actual fpindex"""
        index_name = request.match_info["index"]
        
        try:
            query_data = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        
        # For search, we need to forward to an actual fpindex instance
        # For now, forward to the configured fpindex_url
        async with ClientSession() as session:
            try:
                async with session.post(
                    f"{self.config.fpindex_url}/{index_name}/_search",
                    json=query_data
                ) as resp:
                    result = await resp.json()
                    return web.json_response(result, status=resp.status)
            except Exception as e:
                logger.error(f"Failed to forward search request: {e}")
                return web.json_response({"error": "Search service unavailable"}, status=503)