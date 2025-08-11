"""NATS JetStream consumer service that updates local fpindex instances"""

import asyncio
import logging
from typing import Optional

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
        self.nc: nats.NATS | None = None
        self.js: JetStreamContext | None = None
        self.http_session: Optional[ClientSession] = None
        self.subscription = None

    async def start(self):
        """Start the updater service"""
        logger.info(
            f"Starting updater service with consumer name: {self.config.consumer_name}"
        )

        # Create HTTP session for fpindex updates
        self.http_session = ClientSession()

        # Connect to NATS
        self.nc = await nats.connect(self.config.nats_url)
        self.js = self.nc.jetstream()
        logger.info(f"Connected to NATS at {self.config.nats_url}")

        # Ensure the stream exists (should already be created by proxy)
        await self._ensure_stream(self.config.nats_stream)

        # Create durable consumer
        consumer_config = ConsumerConfig(
            name=self.config.consumer_name,
            deliver_policy=DeliverPolicy.ALL,  # Replay from beginning for new instances
            ack_policy=AckPolicy.EXPLICIT,
        )

        try:
            await self.js.add_consumer(self.config.nats_stream, config=consumer_config)
            logger.info(f"Created consumer: {self.config.consumer_name}")
        except Exception as e:
            if "already exists" not in str(e).lower():
                logger.error(f"Failed to create consumer: {e}")
                raise

        # Subscribe to all fpindex messages
        self.subscription = await self.js.pull_subscribe(
            f"{self.config.nats_stream}.>", durable=self.config.consumer_name
        )

        logger.info("Updater service started, consuming messages...")

        # Start message processing loop
        asyncio.create_task(self._message_processing_loop())

    async def stop(self):
        """Stop the updater service"""
        if self.subscription:
            await self.subscription.unsubscribe()
        if self.http_session:
            await self.http_session.close()
        if self.nc:
            await self.nc.close()
        logger.info("Updater service stopped")

    async def _ensure_stream(self, stream_name: str):
        """Ensure NATS stream exists (should already be created by proxy)"""
        assert self.js is not None
        try:
            stream_info = await self.js.stream_info(stream_name)
            logger.info(
                f"Stream {stream_name} exists with {stream_info.state.messages} messages"
            )
        except Exception:
            logger.warning(
                f"Stream {stream_name} does not exist - will be created by proxy service"
            )

    async def _message_processing_loop(self):
        """Main message processing loop - processes messages in batches"""
        assert self.subscription is not None
        while True:
            try:
                # Fetch messages in batches for efficient bulk updates
                messages = await self.subscription.fetch(batch=50, timeout=5.0)

                if messages:
                    await self._process_message_batch(messages)

            except asyncio.TimeoutError:
                # No messages available, continue loop
                continue
            except Exception as e:
                logger.error(f"Error in message processing loop: {e}")
                await asyncio.sleep(1)  # Brief delay before retrying

    async def _process_message_batch(self, messages):
        """Process a batch of messages using bulk update operations"""
        # Group messages by index name for efficient bulk updates
        index_groups = {}

        for msg in messages:
            try:
                # Parse subject: fpindex.{indexname}.{fpid_hex}
                subject_parts = msg.subject.split(".")
                if len(subject_parts) != 3:
                    logger.warning(f"Invalid subject format: {msg.subject}")
                    await msg.ack()
                    continue

                _, index_name, fpid_hex = subject_parts

                # Convert hex fingerprint ID back to integer
                try:
                    fp_id = int(fpid_hex, 16)
                except ValueError:
                    logger.warning(f"Invalid hex fingerprint ID: {fpid_hex}")
                    await msg.ack()
                    continue

                # Group by index name
                if index_name not in index_groups:
                    index_groups[index_name] = []

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
                        index_groups[index_name].append((change, msg))
                    except Exception as e:
                        logger.error(f"Failed to decode fingerprint data: {e}")
                        await msg.nak()  # Requeue for retry
                        continue
                else:
                    # Empty message = delete
                    change = ChangeDelete(delete=Delete(id=fp_id))
                    index_groups[index_name].append((change, msg))

            except Exception as e:
                logger.error(f"Error parsing message {msg.subject}: {e}")
                await msg.nak()  # Requeue for retry

        # Process each index group with bulk updates
        for index_name, change_msgs in index_groups.items():
            if change_msgs:
                changes = [change for change, msg in change_msgs]
                messages_for_ack = [msg for change, msg in change_msgs]

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
