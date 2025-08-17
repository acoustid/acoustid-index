#!/usr/bin/env python3

import logging
from typing import Union
import nats
from nats.js import JetStreamContext
from nats.js.api import StreamConfig, RetentionPolicy
import msgspec
import aiohttp

from .models import CreateIndexOperation, DeleteIndexOperation, Operation


logger = logging.getLogger(__name__)


class IndexManager:
    """Manages NATS JetStream setup and fpindex operations replay."""
    
    def __init__(self, nats_connection: nats.NATS, js: JetStreamContext, http_session: aiohttp.ClientSession, stream_prefix: str, fpindex_url: str):
        self.nc = nats_connection
        self.js = js
        self.stream_prefix = stream_prefix
        self.stream_name = f"{stream_prefix}_oplog"
        self.subject = f"{stream_prefix}.op.*"
        self.fpindex_url = fpindex_url.rstrip('/')
        self.http_session = http_session
    
    @classmethod
    async def create(cls, nats_connection: nats.NATS, stream_prefix: str = "fpindex", fpindex_url: str = "http://localhost:8080") -> "IndexManager":
        """Create a fully initialized IndexManager."""
        logger.info("Setting up JetStream context")
        js = nats_connection.jetstream()
        
        # Create HTTP session for fpindex communication
        http_session = aiohttp.ClientSession()
        
        # Create the manager instance
        manager = cls(nats_connection, js, http_session, stream_prefix, fpindex_url)
        
        # Set up the stream
        await manager._ensure_stream_exists()
        logger.info(f"JetStream stream '{manager.stream_name}' is ready")
        
        return manager
    
    async def cleanup(self) -> None:
        """Clean up resources."""
        await self.http_session.close()
    
    async def _ensure_stream_exists(self) -> None:
        """Create the fpindex operations stream if it doesn't exist."""
        try:
            # Try to get existing stream info
            await self.js.stream_info(self.stream_name)
            logger.info(f"Stream '{self.stream_name}' already exists")
        except nats.js.errors.NotFoundError:
            # Stream doesn't exist, create it
            logger.info(f"Creating stream '{self.stream_name}'")
            config = StreamConfig(
                name=self.stream_name,
                subjects=[self.subject],
                retention=RetentionPolicy.LIMITS,
                max_msgs=1000000,  # Keep up to 1M messages
                max_age=7 * 24 * 3600,  # Keep messages for 7 days
                storage=nats.js.api.StorageType.FILE,
            )
            await self.js.add_stream(config)
            logger.info(f"Stream '{self.stream_name}' created successfully")
    
    async def replay_operations(self) -> None:
        """Replay all stored operations from the stream on startup."""
        logger.info("Starting operations replay from stream")
        
        try:
            # Get stream info to check if there are any messages
            stream_info = await self.js.stream_info(self.stream_name)
            message_count = stream_info.state.messages
            
            if message_count == 0:
                logger.info("No operations to replay - stream is empty")
                return
            
            logger.info(f"Replaying {message_count} operations from stream")
            
            # Create a consumer to read all messages from the beginning
            consumer_config = nats.js.api.ConsumerConfig(
                deliver_policy=nats.js.api.DeliverPolicy.ALL,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
            )
            
            # Subscribe and process messages
            subscription = await self.js.subscribe(
                self.subject,
                config=consumer_config,
            )
            
            processed = 0
            async for msg in subscription.messages:
                try:
                    await self._process_operation_message(msg)
                    await msg.ack()
                    processed += 1
                    
                    if processed >= message_count:
                        break
                        
                except Exception as e:
                    logger.error(f"Error processing operation message: {e}")
                    await msg.nak()
            
            await subscription.unsubscribe()
            logger.info(f"Successfully replayed {processed} operations")
            
        except Exception as e:
            logger.error(f"Error during operations replay: {e}")
            raise
    
    async def _process_operation_message(self, msg) -> None:
        """Process a single operation message during replay."""
        subject = msg.subject
        data = msg.data
        
        logger.debug(f"Processing operation from subject {subject}, data length: {len(data)}")
        
        try:
            # Extract index name from subject: fpindex.op.{index_name}
            index_name = subject.split('.')[-1]
            
            # Decode operation using Union type
            operation = msgspec.msgpack.decode(data, type=Operation)
            
            if isinstance(operation, CreateIndexOperation):
                await self._apply_create_index(index_name)
            elif isinstance(operation, DeleteIndexOperation):
                await self._apply_delete_index(index_name)
            else:
                logger.warning(f"Unknown operation type: {type(operation)}")
                
        except Exception as e:
            logger.error(f"Error processing operation from subject {subject}: {e}")
            raise
    
    async def _apply_create_index(self, index_name: str) -> None:
        """Apply a CreateIndex operation to fpindex."""
        logger.info(f"Applying CreateIndex operation for index '{index_name}'")
        
        url = f"{self.fpindex_url}/{index_name}"
        try:
            async with self.http_session.put(url) as response:
                if response.status == 200:
                    logger.info(f"Successfully created index '{index_name}'")
                elif response.status == 409:
                    logger.info(f"Index '{index_name}' already exists")
                else:
                    response_text = await response.text()
                    logger.error(f"Failed to create index '{index_name}': {response.status} {response_text}")
                    raise RuntimeError(f"HTTP {response.status}: {response_text}")
        except Exception as e:
            logger.error(f"Error creating index '{index_name}': {e}")
            raise
    
    async def _apply_delete_index(self, index_name: str) -> None:
        """Apply a DeleteIndex operation to fpindex."""
        logger.info(f"Applying DeleteIndex operation for index '{index_name}'")
        
        url = f"{self.fpindex_url}/{index_name}"
        try:
            async with self.http_session.delete(url) as response:
                if response.status == 200:
                    logger.info(f"Successfully deleted index '{index_name}'")
                elif response.status == 404:
                    logger.info(f"Index '{index_name}' does not exist")
                else:
                    response_text = await response.text()
                    logger.error(f"Failed to delete index '{index_name}': {response.status} {response_text}")
                    raise RuntimeError(f"HTTP {response.status}: {response_text}")
        except Exception as e:
            logger.error(f"Error deleting index '{index_name}': {e}")
            raise
    
    async def publish_create_index(self, index_name: str) -> None:
        """Publish a CreateIndex operation."""
        operation = CreateIndexOperation()
        data = msgspec.msgpack.encode(operation)
        await self._publish_operation(index_name, data)
        logger.info(f"Published CreateIndex operation for index '{index_name}'")
    
    async def publish_delete_index(self, index_name: str) -> None:
        """Publish a DeleteIndex operation."""
        operation = DeleteIndexOperation()
        data = msgspec.msgpack.encode(operation)
        await self._publish_operation(index_name, data)
        logger.info(f"Published DeleteIndex operation for index '{index_name}'")
    
    async def _publish_operation(self, index_name: str, data: bytes) -> None:
        """Publish a new operation to the stream."""
        subject = f"{self.stream_prefix}.op.{index_name}"
        
        try:
            ack = await self.js.publish(subject, data)
            logger.debug(f"Published operation to {subject}, seq: {ack.seq}")
        except Exception as e:
            logger.error(f"Error publishing operation to {subject}: {e}")
            raise