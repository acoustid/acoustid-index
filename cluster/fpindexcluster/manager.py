#!/usr/bin/env python3

import asyncio
import logging
import nats
from nats.js import JetStreamContext
from nats.js.api import StreamConfig, RetentionPolicy
import msgspec
import aiohttp
import uuid

from .models import (
    CreateIndexOperation,
    DeleteIndexOperation,
    Operation,
    IndexCreatingEvent,
    IndexCreatedEvent,
    IndexDeletingEvent,
    IndexDeletedEvent,
    DiscoveryEvent,
    IndexState,
    get_index_state,
)


logger = logging.getLogger(__name__)


class IndexManager:
    """Manages NATS JetStream setup and fpindex operations replay."""

    def __init__(
        self,
        nats_connection: nats.NATS,
        js: JetStreamContext,
        http_session: aiohttp.ClientSession,
        stream_prefix: str,
        fpindex_url: str,
        instance_name: str = None,
    ):
        self.nc = nats_connection
        self.js = js
        self.stream_prefix = stream_prefix
        self.fpindex_url = fpindex_url.rstrip("/")
        self.http_session = http_session
        self.instance_name = instance_name
        self.discovery_stream_name = f"{stream_prefix}_discovery"
        self.discovery_subject_pattern = f"{stream_prefix}.discovery.index.*"

        # In-memory cache of index states
        self.index_states: dict[str, IndexState] = {}
        self.index_states_lock = asyncio.Lock()

        # Index discovery subscription
        self.discovery_subscription: (
            nats.js.JetStreamContext.PushSubscription | None
        ) = None

        # Per-index operation stream subscriptions
        self.index_subscriptions: dict[
            str, nats.js.JetStreamContext.PushSubscription
        ] = {}
        self.index_subscriptions_lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        nats_connection: nats.NATS,
        stream_prefix: str = "fpindex",
        fpindex_url: str = "http://localhost:8080",
        instance_name: str = None,
    ) -> "IndexManager":
        """Create a fully initialized IndexManager."""
        logger.info("Setting up JetStream context")
        js = nats_connection.jetstream()

        # Create HTTP session for fpindex communication
        http_session = aiohttp.ClientSession()

        # Create the manager instance
        manager = cls(
            nats_connection, js, http_session, stream_prefix, fpindex_url, instance_name
        )

        # Set up the discovery stream
        await manager._ensure_discovery_stream_exists()

        # Start discovery stream subscription to maintain state cache
        await manager._start_discovery_subscription()

        logger.info("IndexManager ready for per-index stream management")

        return manager

    async def cleanup(self) -> None:
        """Clean up resources."""
        # Stop discovery subscription
        if self.discovery_subscription:
            await self.discovery_subscription.unsubscribe()

        # Stop all index subscriptions
        async with self.index_subscriptions_lock:
            for index_name, subscription in list(self.index_subscriptions.items()):
                try:
                    await subscription.unsubscribe()
                    logger.debug(
                        f"Stopped subscription for index '{index_name}' during cleanup"
                    )
                except Exception as e:
                    logger.error(
                        f"Error stopping subscription for index '{index_name}' during cleanup: {e}"
                    )
            self.index_subscriptions.clear()

        await self.http_session.close()

    def _get_stream_name(self, index_name: str) -> str:
        """Get stream name for a specific index."""
        return f"{self.stream_prefix}_{index_name}_oplog"

    def _get_subject(self, index_name: str) -> str:
        """Get subject for a specific index."""
        return f"{self.stream_prefix}.{index_name}.op"

    def _get_discovery_subject(self, index_name: str) -> str:
        """Get discovery subject for index lifecycle events."""
        return f"{self.stream_prefix}.discovery.index.{index_name}"

    async def _ensure_stream_exists(self, index_name: str) -> None:
        """Create the stream for a specific index if it doesn't exist."""
        stream_name = self._get_stream_name(index_name)
        subject = self._get_subject(index_name)

        try:
            # Try to get existing stream info
            await self.js.stream_info(stream_name)
            logger.debug(f"Stream '{stream_name}' already exists")
        except nats.js.errors.NotFoundError:
            # Stream doesn't exist, create it
            logger.info(f"Creating stream '{stream_name}' for index '{index_name}'")
            config = StreamConfig(
                name=stream_name,
                subjects=[subject],
                retention=RetentionPolicy.LIMITS,
                max_msgs=1000000,  # Keep up to 1M messages
                max_age=7 * 24 * 3600,  # Keep messages for 7 days
                storage=nats.js.api.StorageType.FILE,
            )
            await self.js.add_stream(config)
            logger.info(f"Stream '{stream_name}' created successfully")

    async def _ensure_discovery_stream_exists(self) -> None:
        """Create the global discovery stream if it doesn't exist."""
        try:
            # Try to get existing stream info
            await self.js.stream_info(self.discovery_stream_name)
            logger.debug(
                f"Discovery stream '{self.discovery_stream_name}' already exists"
            )
        except nats.js.errors.NotFoundError:
            # Stream doesn't exist, create it
            logger.info(f"Creating discovery stream '{self.discovery_stream_name}'")
            config = StreamConfig(
                name=self.discovery_stream_name,
                subjects=[self.discovery_subject_pattern],
                retention=RetentionPolicy.WORK_QUEUE,
                max_msgs_per_subject=1,  # Keep only latest event per subject (per index)
                storage=nats.js.api.StorageType.FILE,
            )
            await self.js.add_stream(config)
            logger.info(
                f"Discovery stream '{self.discovery_stream_name}' created successfully"
            )

    async def _start_discovery_subscription(self) -> None:
        """Start persistent subscription to discovery stream to maintain index state cache."""
        try:
            # Single consumer that gets only the latest message per subject then continues with new ones
            consumer_config = nats.js.api.ConsumerConfig(
                deliver_policy=nats.js.api.DeliverPolicy.LAST_PER_SUBJECT,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
            )

            self.discovery_subscription = await self.js.subscribe(
                self.discovery_subject_pattern,
                config=consumer_config,
                cb=self._discovery_message_callback,
            )

            logger.info("Started discovery stream subscription with callback")

        except Exception as e:
            logger.error(f"Error starting discovery subscription: {e}")
            raise

    async def _discovery_message_callback(self, msg: nats.aio.msg.Msg) -> None:
        """Callback to process discovery messages and update state cache."""
        try:
            await self._process_discovery_message(msg)
            await msg.ack()
        except Exception as e:
            logger.error(f"Error processing discovery message: {e}")
            await msg.nak()

    async def _process_discovery_message(self, msg: nats.aio.msg.Msg) -> None:
        """Process a single discovery message and update state cache."""
        # Extract index name from subject: {prefix}.discovery.index.{index_name}
        subject_parts = msg.subject.split(".")
        if (
            len(subject_parts) == 4
            and subject_parts[1] == "discovery"
            and subject_parts[2] == "index"
        ):
            index_name = subject_parts[3]

            # Decode the event
            event = msgspec.msgpack.decode(msg.data, type=DiscoveryEvent)
            new_state = get_index_state(event)

            # Update cache
            async with self.index_states_lock:
                old_state = self.index_states.get(index_name, IndexState.NOT_EXISTS)
                self.index_states[index_name] = new_state

            logger.debug(
                f"Updated state for index '{index_name}': {old_state.value} -> {new_state.value}"
            )

            # Manage per-index subscriptions based on state changes
            await self._manage_index_subscription(index_name, new_state)

    async def _manage_index_subscription(
        self, index_name: str, new_state: IndexState
    ) -> None:
        """Start or stop per-index subscriptions based on state changes."""
        if new_state.exists():
            # Start subscription when index becomes active
            await self._start_index_subscription(index_name)
        else:
            # Stop subscription when index is deleted or no longer exists
            await self._stop_index_subscription(index_name)

    async def _start_index_subscription(self, index_name: str) -> None:
        """Start a persistent subscription to an index's operation stream."""
        async with self.index_subscriptions_lock:
            # Check if already subscribed
            if index_name in self.index_subscriptions:
                logger.debug(f"Already subscribed to index '{index_name}' operations")
                return

        try:
            # Ensure stream exists
            await self._ensure_stream_exists(index_name)

            subject = self._get_subject(index_name)

            # Create consumer that gets new messages (not replay)
            consumer_config = nats.js.api.ConsumerConfig(
                deliver_policy=nats.js.api.DeliverPolicy.ALL,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                backoff=[0.01, 0.05, 0.1, 0.5, 1.0],
            )

            subscription = await self.js.subscribe(
                subject,
                config=consumer_config,
                durable=f"fpindex-{index_name}-{self.instance_name}",
                cb=lambda msg: self._index_operation_callback(msg, index_name),
            )

            async with self.index_subscriptions_lock:
                self.index_subscriptions[index_name] = subscription

            logger.info(f"Started subscription to index '{index_name}' operations")

        except Exception:
            logger.exception(f"Error starting subscription for index '{index_name}'")
            raise

    async def _stop_index_subscription(self, index_name: str) -> None:
        """Stop the subscription to an index's operation stream."""
        async with self.index_subscriptions_lock:
            subscription = self.index_subscriptions.pop(index_name, None)

        if subscription:
            try:
                await subscription.unsubscribe()
                logger.info(f"Stopped subscription to index '{index_name}' operations")
            except Exception as e:
                logger.error(
                    f"Error stopping subscription for index '{index_name}': {e}"
                )

    async def _index_operation_callback(
        self, msg: nats.aio.msg.Msg, index_name: str
    ) -> None:
        """Callback to process real-time operation messages for an index."""
        try:
            await self._process_operation_message(msg, index_name)
            await msg.ack()
        except Exception as e:
            logger.error(
                f"Error processing operation message for index '{index_name}': {e}"
            )
            await msg.nak()

    async def get_index_state(self, index_name: str) -> IndexState:
        """Get the current state of an index from the in-memory cache."""
        async with self.index_states_lock:
            return self.index_states.get(index_name, IndexState.NOT_EXISTS)

    async def _process_operation_message(
        self, msg: nats.aio.msg.Msg, index_name: str
    ) -> None:
        """Process a single operation message for an index."""
        subject = msg.subject
        data = msg.data

        logger.debug(
            f"Processing operation from subject {subject} for index '{index_name}', data length: {len(data)}"
        )

        # Decode operation using Union type
        operation = msgspec.msgpack.decode(data, type=Operation)

        if isinstance(operation, CreateIndexOperation):
            await self._apply_create_index(index_name)
        elif isinstance(operation, DeleteIndexOperation):
            await self._apply_delete_index(index_name)
        else:
            logger.warning(f"Unknown operation type: {type(operation)}")

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
                    logger.error(
                        f"Failed to create index '{index_name}': {response.status} {response_text}"
                    )
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
                    logger.error(
                        f"Failed to delete index '{index_name}': {response.status} {response_text}"
                    )
                    raise RuntimeError(f"HTTP {response.status}: {response_text}")
        except Exception as e:
            logger.error(f"Error deleting index '{index_name}': {e}")
            raise

    async def publish_create_index(
        self, index_name: str
    ) -> tuple[bool, str, IndexState]:
        """
        Publish a CreateIndex operation with state validation.
        Returns (success, message, current_state).
        """
        # Check current state
        current_state = await self.get_index_state(index_name)

        # Validate state transition
        if current_state in [IndexState.CREATING, IndexState.ACTIVE]:
            return (
                False,
                f"Index '{index_name}' already exists or is being created",
                current_state,
            )
        elif current_state == IndexState.DELETING:
            return (
                False,
                f"Index '{index_name}' is currently being deleted",
                current_state,
            )

        # Generate operation ID for tracking
        operation_id = str(uuid.uuid4())

        try:
            # Publish "creating" state first
            await self._publish_discovery_event(
                index_name, IndexCreatingEvent(operation_id=operation_id)
            )

            # Ensure stream exists for this index
            await self._ensure_stream_exists(index_name)

            # Publish the operation to the index stream
            operation = CreateIndexOperation()
            data = msgspec.msgpack.encode(operation)
            await self._publish_operation(index_name, data)

            # Publish "created" state
            await self._publish_discovery_event(
                index_name, IndexCreatedEvent(operation_id=operation_id)
            )

            logger.info(
                f"Published CreateIndex operation for index '{index_name}' with ID {operation_id}"
            )
            return True, "Index creation initiated", IndexState.ACTIVE

        except Exception as e:
            logger.error(f"Error creating index '{index_name}': {e}")
            # TODO: Publish error state or retry logic
            raise

    async def publish_delete_index(
        self, index_name: str
    ) -> tuple[bool, str, IndexState]:
        """
        Publish a DeleteIndex operation with state validation.
        Returns (success, message, current_state).
        """
        # Check current state
        current_state = await self.get_index_state(index_name)

        # Validate state transition
        if current_state in [IndexState.NOT_EXISTS, IndexState.DELETED]:
            # Idempotent: deleting non-existent index is success
            return (
                True,
                f"Index '{index_name}' does not exist (already deleted)",
                current_state,
            )
        elif current_state == IndexState.CREATING:
            return (
                False,
                f"Index '{index_name}' is currently being created",
                current_state,
            )
        elif current_state == IndexState.DELETING:
            return (
                False,
                f"Index '{index_name}' is already being deleted",
                current_state,
            )

        # Generate operation ID for tracking
        operation_id = str(uuid.uuid4())

        try:
            # Publish "deleting" state first
            await self._publish_discovery_event(
                index_name, IndexDeletingEvent(operation_id=operation_id)
            )

            # Ensure stream exists for this index (might need to publish to existing stream)
            await self._ensure_stream_exists(index_name)

            # Publish the operation to the index stream
            operation = DeleteIndexOperation()
            data = msgspec.msgpack.encode(operation)
            await self._publish_operation(index_name, data)

            # Publish "deleted" state
            await self._publish_discovery_event(
                index_name, IndexDeletedEvent(operation_id=operation_id)
            )

            logger.info(
                f"Published DeleteIndex operation for index '{index_name}' with ID {operation_id}"
            )
            return True, "Index deletion initiated", IndexState.DELETED

        except Exception as e:
            logger.error(f"Error deleting index '{index_name}': {e}")
            # TODO: Publish error state or retry logic
            raise

    async def _publish_operation(self, index_name: str, data: bytes) -> None:
        """Publish a new operation to the index-specific stream."""
        subject = self._get_subject(index_name)

        try:
            ack = await self.js.publish(subject, data)
            logger.debug(f"Published operation to {subject}, seq: {ack.seq}")
        except Exception as e:
            logger.error(f"Error publishing operation to {subject}: {e}")
            raise

    async def _publish_discovery_event(
        self, index_name: str, event: DiscoveryEvent
    ) -> None:
        """Publish a discovery event to the global discovery stream."""
        subject = self._get_discovery_subject(index_name)
        data = msgspec.msgpack.encode(event)

        try:
            ack = await self.js.publish(subject, data)
            logger.debug(f"Published discovery event to {subject}, seq: {ack.seq}")
        except Exception as e:
            logger.error(f"Error publishing discovery event to {subject}: {e}")
            raise
