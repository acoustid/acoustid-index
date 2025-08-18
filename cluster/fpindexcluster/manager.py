#!/usr/bin/env python3

import asyncio
import logging
import nats
import nats.js
import nats.js.api
import nats.js.errors
import nats.errors
import nats.aio.msg
from nats.js import JetStreamContext
from nats.js.api import StreamConfig, RetentionPolicy, Header
import msgspec
import msgspec.msgpack
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


class WrongLastSequence(Exception):
    pass


class IndexUpdater:
    """Handles NATS pull subscription and message processing for a single index."""

    def __init__(
        self,
        index_name: str,
        js: JetStreamContext,
        http_session: aiohttp.ClientSession,
        stream_name: str,
        subject: str,
        fpindex_url: str,
        instance_name: str,
    ):
        self.index_name = index_name
        self.js = js
        self.http_session = http_session
        self.stream_name = stream_name
        self.subject = subject
        self.fpindex_url = fpindex_url
        self.instance_name = instance_name

        self.subscription: nats.js.JetStreamContext.PullSubscription | None = None
        self.pull_task: asyncio.Task | None = None
        self.shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the pull consumer and message processing."""
        if self.subscription is not None:
            logger.debug(f"IndexUpdater for '{self.index_name}' already started")
            return

        try:
            # Create durable pull consumer
            consumer_config = nats.js.api.ConsumerConfig(
                durable_name=f"fpindex-{self.index_name}-{self.instance_name}",
                deliver_policy=nats.js.api.DeliverPolicy.ALL,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                max_waiting=512,
                max_ack_pending=1000,
            )

            self.subscription = await self.js.pull_subscribe(
                self.subject,
                durable=f"fpindex-{self.index_name}-{self.instance_name}",
                stream=self.stream_name,
                config=consumer_config,
            )

            # Start background task to continuously pull messages
            self.pull_task = asyncio.create_task(self._pull_messages_continuously())

            logger.info(f"Started IndexUpdater for '{self.index_name}'")

        except Exception:
            logger.exception(f"Error starting IndexUpdater for '{self.index_name}'")
            raise

    async def stop(self) -> None:
        """Stop the pull consumer gracefully."""
        logger.info(f"Stopping IndexUpdater for '{self.index_name}'")

        # Signal shutdown
        self.shutdown_event.set()

        if self.pull_task:
            try:
                # Wait for graceful shutdown
                await asyncio.wait_for(self.pull_task, timeout=5.0)
                logger.debug(f"Gracefully stopped pull task for '{self.index_name}'")
            except asyncio.TimeoutError:
                logger.warning(f"Pull task for '{self.index_name}' didn't stop gracefully, cancelling")
                self.pull_task.cancel()
                try:
                    await self.pull_task
                except asyncio.CancelledError:
                    pass
            except Exception as e:
                logger.error(f"Error stopping pull task for '{self.index_name}': {e}")
            finally:
                self.pull_task = None

        if self.subscription:
            try:
                await self.subscription.unsubscribe()
                logger.debug(f"Unsubscribed IndexUpdater for '{self.index_name}'")
            except Exception as e:
                logger.error(f"Error unsubscribing IndexUpdater for '{self.index_name}': {e}")
            finally:
                self.subscription = None

    async def _pull_messages_continuously(self) -> None:
        """Continuously pull messages from the subscription until shutdown."""
        logger.info(f"Starting continuous message pulling for '{self.index_name}'")

        try:
            while not self.shutdown_event.is_set():
                try:
                    # Pull up to 10 messages with a 5 second timeout
                    messages = await self.subscription.fetch(batch=10, timeout=5)

                    for msg in messages:
                        # Check for shutdown between messages
                        if self.shutdown_event.is_set():
                            logger.info(f"Shutdown signaled for '{self.index_name}', stopping message processing")
                            break

                        try:
                            await self._process_operation_message(msg)
                            await msg.ack()
                        except Exception as e:
                            logger.error(f"Error processing operation message for '{self.index_name}': {e}")
                            await msg.nak()

                except nats.errors.TimeoutError:
                    # No messages available, continue polling (but check shutdown)
                    continue
                except Exception as e:
                    logger.error(f"Error fetching messages for '{self.index_name}': {e}")
                    # Wait a bit before retrying, but check for shutdown
                    try:
                        await asyncio.wait_for(self.shutdown_event.wait(), timeout=1.0)
                        break  # Shutdown was signaled
                    except asyncio.TimeoutError:
                        continue  # No shutdown signal, continue retrying

            logger.info(f"Message pulling stopped gracefully for '{self.index_name}'")

        except asyncio.CancelledError:
            logger.info(f"Message pulling cancelled for '{self.index_name}'")
            raise
        except Exception as e:
            logger.error(f"Fatal error in message pulling for '{self.index_name}': {e}")
            raise

    async def _process_operation_message(self, msg: nats.aio.msg.Msg) -> None:
        """Process a single operation message for this index."""
        subject = msg.subject
        data = msg.data

        logger.debug(f"Processing operation from subject {subject} for '{self.index_name}', data length: {len(data)}")

        # Decode operation using Union type
        operation = msgspec.msgpack.decode(data, type=Operation)

        if isinstance(operation, CreateIndexOperation):
            await self._apply_create_index()
        elif isinstance(operation, DeleteIndexOperation):
            await self._apply_delete_index()
        else:
            logger.warning(f"Unknown operation type: {type(operation)}")

    async def _apply_create_index(self) -> None:
        """Apply a CreateIndex operation to fpindex."""
        logger.info(f"Applying CreateIndex operation for '{self.index_name}'")

        url = f"{self.fpindex_url}/{self.index_name}"
        try:
            async with self.http_session.put(url) as response:
                if response.status == 200:
                    logger.info(f"Successfully created index '{self.index_name}'")
                elif response.status == 409:
                    logger.info(f"Index '{self.index_name}' already exists")
                else:
                    response_text = await response.text()
                    logger.error(f"Failed to create index '{self.index_name}': {response.status} {response_text}")
                    raise RuntimeError(f"HTTP {response.status}: {response_text}")
        except Exception as e:
            logger.error(f"Error creating index '{self.index_name}': {e}")
            raise

    async def _apply_delete_index(self) -> None:
        """Apply a DeleteIndex operation to fpindex."""
        logger.info(f"Applying DeleteIndex operation for '{self.index_name}'")

        url = f"{self.fpindex_url}/{self.index_name}"
        try:
            async with self.http_session.delete(url) as response:
                if response.status == 200:
                    logger.info(f"Successfully deleted index '{self.index_name}'")
                elif response.status == 404:
                    logger.info(f"Index '{self.index_name}' does not exist")
                else:
                    response_text = await response.text()
                    logger.error(f"Failed to delete index '{self.index_name}': {response.status} {response_text}")
                    raise RuntimeError(f"HTTP {response.status}: {response_text}")
        except Exception as e:
            logger.error(f"Error deleting index '{self.index_name}': {e}")
            raise


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
        instance_name: str,
    ):
        self.nc = nats_connection
        self.js = js
        self.stream_prefix = stream_prefix
        self.fpindex_url = fpindex_url.rstrip("/")
        self.http_session = http_session
        self.instance_name = instance_name
        self.discovery_stream_name = f"{stream_prefix}_discovery"
        self.discovery_subject_pattern = f"{stream_prefix}.discovery.index.*"

        # Integrated index state and sequence management
        self.index_states: dict[str, tuple[IndexState, int | None]] = {}
        self.index_states_lock = asyncio.Lock()

        # Index discovery subscription
        self.discovery_subscription: nats.js.JetStreamContext.PushSubscription | None = None

        # Per-index updaters
        self.index_updaters: dict[str, IndexUpdater] = {}
        self.index_updaters_lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        *,
        nats_connection: nats.NATS,
        stream_prefix: str,
        fpindex_url: str,
        instance_name: str,
    ) -> "IndexManager":
        """Create a fully initialized IndexManager."""
        logger.info("Setting up JetStream context")
        js = nats_connection.jetstream()

        # Create HTTP session for fpindex communication
        http_session = aiohttp.ClientSession()

        # Create the manager instance
        manager = cls(nats_connection, js, http_session, stream_prefix, fpindex_url, instance_name)

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

        # Stop all index updaters
        async with self.index_updaters_lock:
            for index_name, updater in list(self.index_updaters.items()):
                try:
                    await updater.stop()
                    logger.debug(f"Stopped updater for index '{index_name}' during cleanup")
                except Exception as e:
                    logger.error(f"Error stopping updater for index '{index_name}' during cleanup: {e}")
            self.index_updaters.clear()

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
                max_age=(7 * 24 * 3600),  # Keep messages for 7 days
                storage=nats.js.api.StorageType.FILE,
            )
            await self.js.add_stream(config)
            logger.info(f"Stream '{stream_name}' created successfully")

    async def _ensure_discovery_stream_exists(self) -> None:
        """Create the global discovery stream if it doesn't exist."""
        try:
            # Try to get existing stream info
            await self.js.stream_info(self.discovery_stream_name)
            logger.debug(f"Discovery stream '{self.discovery_stream_name}' already exists")
        except nats.js.errors.NotFoundError:
            # Stream doesn't exist, create it
            logger.info(f"Creating discovery stream '{self.discovery_stream_name}'")
            config = StreamConfig(
                name=self.discovery_stream_name,
                subjects=[self.discovery_subject_pattern],
                retention=RetentionPolicy.LIMITS,
                max_msgs_per_subject=1,  # Keep only latest event per subject (per index)
                storage=nats.js.api.StorageType.FILE,
            )
            await self.js.add_stream(config)
            logger.info(f"Discovery stream '{self.discovery_stream_name}' created successfully")

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
        if len(subject_parts) == 4 and subject_parts[1] == "discovery" and subject_parts[2] == "index":
            index_name = subject_parts[3]

            # Decode the event
            event = msgspec.msgpack.decode(msg.data, type=DiscoveryEvent)
            new_state = get_index_state(event)
            new_seq = msg.metadata.sequence.stream

            # Update integrated state cache
            async with self.index_states_lock:
                old_state, old_seq = self.index_states.get(index_name, (IndexState.NOT_EXISTS, None))
                self.index_states[index_name] = (new_state, new_seq)

            logger.debug(
                f"Updated state for index '{index_name}': {old_state.value} -> {new_state.value} (sequence {new_seq})"
            )

            # Manage per-index subscriptions based on state changes
            await self._manage_index_subscription(index_name, new_state)

    async def _manage_index_subscription(self, index_name: str, new_state: IndexState) -> None:
        """Start or stop per-index subscriptions based on state changes."""
        if new_state.exists():
            # Start subscription when index becomes active
            await self._start_index_subscription(index_name)
        else:
            # Stop subscription when index is deleted or no longer exists
            await self._stop_index_subscription(index_name)

    async def _start_index_subscription(self, index_name: str) -> None:
        """Start an IndexUpdater for an index's operation stream."""
        async with self.index_updaters_lock:
            # Check if already started
            if index_name in self.index_updaters:
                logger.debug(f"Already have updater for index '{index_name}' operations")
                return

        try:
            # Ensure stream exists
            await self._ensure_stream_exists(index_name)

            stream_name = self._get_stream_name(index_name)
            subject = self._get_subject(index_name)

            # Create and start IndexUpdater
            updater = IndexUpdater(
                index_name=index_name,
                js=self.js,
                http_session=self.http_session,
                stream_name=stream_name,
                subject=subject,
                fpindex_url=self.fpindex_url,
                instance_name=self.instance_name,
            )

            await updater.start()

            async with self.index_updaters_lock:
                self.index_updaters[index_name] = updater

            logger.info(f"Started IndexUpdater for index '{index_name}' operations")

        except Exception:
            logger.exception(f"Error starting IndexUpdater for index '{index_name}'")
            raise

    async def _stop_index_subscription(self, index_name: str) -> None:
        """Stop the IndexUpdater for an index's operation stream."""
        async with self.index_updaters_lock:
            updater = self.index_updaters.pop(index_name, None)

        if updater:
            try:
                await updater.stop()
                logger.info(f"Stopped IndexUpdater for index '{index_name}' operations")
            except Exception as e:
                logger.error(f"Error stopping IndexUpdater for index '{index_name}': {e}")

    async def get_index_state(self, index_name: str) -> IndexState:
        """Get the current state of an index from the in-memory cache."""
        async with self.index_states_lock:
            state, _ = self.index_states.get(index_name, (IndexState.NOT_EXISTS, None))
            return state

    async def _retry_on_conflict(self, operation, max_retries=3, base_delay=0.1):
        """Retry an operation on optimistic lock conflicts with exponential backoff."""
        import random

        for attempt in range(max_retries):
            try:
                return await operation()
            except nats.js.errors.Error:
                if attempt == max_retries - 1:
                    raise

                # Exponential backoff with jitter
                delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
                logger.warning(
                    f"Optimistic lock conflict, retrying in {delay:.2f}s (attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(delay)

    async def publish_create_index(self, index_name: str) -> tuple[bool, str, IndexState]:
        """
        Publish a CreateIndex operation with state validation and optimistic locking.
        Returns (success, message, current_state).
        """
        # Generate operation ID for tracking
        operation_id = str(uuid.uuid4())

        async def _create_index_operation():
            # Get current sequence for optimistic locking
            async with self.index_states_lock:
                current_state, current_sequence = self.index_states.get(index_name, (IndexState.NOT_EXISTS, None))

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

            # Publish "creating" state first
            updated_sequence = await self._publish_discovery_event(
                index_name,
                IndexCreatingEvent(operation_id=operation_id),
                expected_sequence=current_sequence,
            )

            # Ensure stream exists for this index
            await self._ensure_stream_exists(index_name)

            # Publish the operation to the index stream
            operation = CreateIndexOperation()
            data = msgspec.msgpack.encode(operation)
            await self._publish_operation(index_name, data)

            # Publish "created" state
            await self._publish_discovery_event(
                index_name,
                IndexCreatedEvent(operation_id=operation_id),
                expected_sequence=updated_sequence,
            )

            logger.info(f"Published CreateIndex operation for index '{index_name}' with ID {operation_id}")
            return True, "Index creation initiated", IndexState.ACTIVE

        return await self._retry_on_conflict(_create_index_operation)

    async def publish_delete_index(self, index_name: str) -> tuple[bool, str, IndexState]:
        """
        Publish a DeleteIndex operation with state validation and optimistic locking.
        Returns (success, message, current_state).
        """
        # Check current state
        current_state = await self.get_index_state(index_name)

        # Generate operation ID for tracking
        operation_id = str(uuid.uuid4())

        async def _delete_index_operation():
            # Get current sequence for optimistic locking
            async with self.index_states_lock:
                current_state, current_sequence = self.index_states.get(index_name, (IndexState.NOT_EXISTS, None))

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

            # Publish "deleting" state first
            updated_sequence = await self._publish_discovery_event(
                index_name,
                IndexDeletingEvent(operation_id=operation_id),
                expected_sequence=current_sequence,
            )

            # Ensure stream exists for this index (might need to publish to existing stream)
            await self._ensure_stream_exists(index_name)

            # Publish the operation to the index stream
            operation = DeleteIndexOperation()
            data = msgspec.msgpack.encode(operation)
            await self._publish_operation(index_name, data)

            # Publish "deleted" state
            await self._publish_discovery_event(
                index_name,
                IndexDeletedEvent(operation_id=operation_id),
                expected_sequence=updated_sequence,
            )

            logger.info(f"Published DeleteIndex operation for index '{index_name}' with ID {operation_id}")
            return True, "Index deletion initiated", IndexState.DELETED

        return await self._retry_on_conflict(_delete_index_operation)

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
        self, index_name: str, event: DiscoveryEvent, expected_sequence: int | None = None
    ) -> int:
        """Publish a discovery event to the global discovery stream with optimistic locking."""
        subject = self._get_discovery_subject(index_name)
        data = msgspec.msgpack.encode(event)

        try:
            headers = {}
            headers[Header.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(expected_sequence or 0)

            ack = await self.js.publish(subject, data, headers=headers)

            logger.debug(f"Published discovery event to {subject}, seq: {ack.seq}")
            return ack.seq
        except nats.js.errors.APIError as err:
            # Check for a BadRequest::KeyWrongLastSequenceError error code.
            if err.err_code == 10071:
                raise WrongLastSequence()
            else:
                raise
