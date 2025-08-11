"""NATS JetStream consumer service that updates local fpindex instances"""

import asyncio
import logging
from contextlib import AsyncExitStack
from typing import Optional, Any

import msgspec.msgpack
import nats
from aiohttp import ClientSession
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy

from .config import Config
from .models import (
    FingerprintData,
    BulkUpdateRequest,
    ChangeInsert,
    ChangeDelete,
    Insert,
    Delete,
)

logger = logging.getLogger(__name__)


class UpdaterService:
    """Sidecar consumer that reads from NATS and updates local fpindex using bulk operations"""

    def __init__(self, config: Config):
        self.config = config
        self.subscriptions: dict[str, object] = {}  # index_name -> subscription
        self.tracked_indexes: set[str] = set()
        self._exit_stack: Optional[AsyncExitStack] = None
        
        # These will be set during async context manager entry
        self.nc: Any  # nats.NATS connection
        self.js: JetStreamContext  
        self.http_session: ClientSession

    async def __aenter__(self):
        """Async context manager entry - initialize resources"""
        logger.info(
            f"Initializing updater service with consumer name: {self.config.consumer_name}"
        )
        
        self._exit_stack = AsyncExitStack()
        
        # Create HTTP session for fpindex updates
        self.http_session = await self._exit_stack.enter_async_context(
            ClientSession()
        )
        
        # Connect to NATS
        self.nc = await nats.connect(self.config.nats_url)
        # Register NATS connection for cleanup
        self._exit_stack.callback(self.nc.close)
        self.js = self.nc.jetstream()
        logger.info(f"Connected to NATS at {self.config.nats_url}")
        
        logger.info("Updater service resources initialized")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup resources"""
        # Unsubscribe from all subscriptions
        for subscription in self.subscriptions.values():
            try:
                await subscription.unsubscribe()  # type: ignore
            except Exception as e:
                logger.error(f"Error unsubscribing: {e}")
        
        
        # Close all resources via exit stack
        if self._exit_stack:
            await self._exit_stack.aclose()
        
        logger.info("Updater service stopped")

    async def run(self):
        """Run the updater service - must be called within async context manager"""
        if not hasattr(self, 'nc') or not hasattr(self, 'js'):
            raise RuntimeError("UpdaterService.run() must be called within async context manager")
        
        logger.info("Starting updater service...")
        
        
        # Discover and subscribe to existing indexes
        await self._discover_and_subscribe_indexes()
        
        logger.info("Updater service running, consuming messages...")
        
        # Keep running until context manager exits
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Updater service shutting down...")
            raise
    


    async def _discover_and_subscribe_indexes(self):
        """Discover existing index streams and subscribe"""
        try:
            streams = await self.js.streams_info()
            for stream in streams:
                if stream.config.name.startswith(self.config.nats_stream_prefix + "-"):
                    index_name = stream.config.name[len(self.config.nats_stream_prefix + "-"):]
                    if index_name not in self.tracked_indexes:
                        await self._subscribe_to_index(index_name)
        except Exception as e:
            logger.error(f"Failed to discover indexes: {e}")

    async def _subscribe_to_index(self, index_name: str):
        """Subscribe to a specific index"""
        stream_name = self.config.get_stream_name(index_name)
        consumer_name = f"{self.config.consumer_name}-{index_name}"
        
        consumer_config = ConsumerConfig(
            name=consumer_name,
            deliver_policy=DeliverPolicy.ALL,
            ack_policy=AckPolicy.EXPLICIT,
        )
        
        try:
            await self.js.add_consumer(stream_name, config=consumer_config)
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise
        
        subscription = await self.js.pull_subscribe(
            f"fpindex.{index_name}.>", durable=consumer_name
        )
        
        self.subscriptions[index_name] = subscription
        self.tracked_indexes.add(index_name)
        
        # Start processing loop for this index
        asyncio.create_task(self._message_processing_loop_for_index(index_name))
        logger.info(f"Subscribed to index: {index_name}")

    async def _unsubscribe_from_index(self, index_name: str):
        """Unsubscribe from a deleted index"""
        if index_name in self.subscriptions:
            await self.subscriptions[index_name].unsubscribe()  # type: ignore
            del self.subscriptions[index_name]
            
        self.tracked_indexes.discard(index_name)
        logger.info(f"Unsubscribed from deleted index: {index_name}")



    async def _message_processing_loop_for_index(self, index_name: str):
        """Message processing loop for a specific index"""
        subscription = self.subscriptions[index_name]
        while index_name in self.subscriptions:
            try:
                # Fetch messages in batches for efficient bulk updates
                messages = await subscription.fetch(batch=50, timeout=5.0)

                if messages:
                    await self._process_message_batch(messages, index_name)

            except asyncio.TimeoutError:
                # No messages available, continue loop
                continue
            except Exception as e:
                logger.error(f"Error in message processing loop for {index_name}: {e}")
                await asyncio.sleep(1)  # Brief delay before retrying

    async def _process_message_batch(self, messages, index_name: str):
        """Process a batch of messages using bulk update operations"""
        changes = []
        messages_for_ack = []

        for msg in messages:
            try:
                # Parse subject: fpindex.{indexname}.{fpid_hex}
                subject_parts = msg.subject.split(".")
                if len(subject_parts) != 3:
                    logger.warning(f"Invalid subject format: {msg.subject}")
                    await msg.ack()
                    continue

                _, msg_index_name, fpid_hex = subject_parts
                
                # Sanity check - should match the index we're processing
                if msg_index_name != index_name:
                    logger.warning(f"Message index {msg_index_name} doesn't match expected {index_name}")
                    await msg.ack()
                    continue

                # Convert hex fingerprint ID back to integer
                try:
                    fp_id = int(fpid_hex, 16)
                except ValueError:
                    logger.warning(f"Invalid hex fingerprint ID: {fpid_hex}")
                    await msg.ack()
                    continue

                # Create change object based on message data
                if msg.data:
                    # Non-empty message = insert (fpindex doesn't have separate update)
                    try:
                        fingerprint_data = msgspec.msgpack.decode(
                            msg.data, type=FingerprintData
                        )
                        change = ChangeInsert(
                            insert=Insert(id=fp_id, hashes=fingerprint_data.hashes)
                        )
                        changes.append(change)
                        messages_for_ack.append(msg)
                    except Exception as e:
                        logger.error(f"Failed to decode fingerprint data: {e}")
                        await msg.nak()  # Requeue for retry
                        continue
                else:
                    # Empty message = delete
                    change = ChangeDelete(delete=Delete(id=fp_id))
                    changes.append(change)
                    messages_for_ack.append(msg)

            except Exception as e:
                logger.error(f"Error parsing message {msg.subject}: {e}")
                await msg.nak()  # Requeue for retry

        # Process the batch for this index
        if changes:
            try:
                await self._bulk_update_fpindex(index_name, changes)
                logger.info(
                    f"Bulk updated {len(changes)} changes in index {index_name}"
                )

                # Acknowledge all messages in this batch
                for msg in messages_for_ack:
                    await msg.ack()

            except Exception as e:
                logger.error(f"Bulk update failed for index {index_name}: {e}")

                # Requeue all messages in this batch for retry
                for msg in messages_for_ack:
                    await msg.nak()

    async def _bulk_update_fpindex(self, index_name: str, changes: list):
        """Send bulk update to local fpindex instance"""
        url = f"{self.config.fpindex_url}/{index_name}/_update"

        # Create bulk update request
        bulk_request = BulkUpdateRequest(changes=changes)
        request_data = msgspec.msgpack.encode(bulk_request)

        try:
            async with self.http_session.post(
                url,
                data=request_data,
                headers={"Content-Type": "application/vnd.msgpack"},
            ) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(
                        f"fpindex bulk update failed: {resp.status} - {error_text}"
                    )
                    raise Exception(f"fpindex bulk update failed: {resp.status}")

                # fpindex /_update returns EmptyResponse, no need to parse results
                logger.debug(f"Bulk update succeeded for {len(changes)} changes")

        except Exception as e:
            logger.error(f"HTTP error in bulk update: {e}")
            raise
