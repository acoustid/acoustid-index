#!/usr/bin/env python3

import asyncio
import logging
import nats
import datetime
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
import random

from .models import (
    CreateIndexOperation,
    DeleteIndexOperation,
    Operation,
    IndexStatus,
    IndexStatusChange,
    IndexStatusUpdate,
    DEFAULT_INDEX_STATUS_UPDATE,
    BootstrapQuery,
    BootstrapReply,
)
from .errors import (
    InconsistentIndexState,
)


logger = logging.getLogger(__name__)


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
        manager: "IndexManager",
    ):
        self.index_name = index_name
        self.js = js
        self.http_session = http_session
        self.stream_name = stream_name
        self.subject = subject
        self.fpindex_url = fpindex_url
        self.instance_name = instance_name
        self.manager = manager

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

            # Check if bootstrap is needed before processing messages
            await self._check_bootstrap_needed()
            
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
        except Exception:
            logger.exception("Error deleting index %s", self.index_name)
            raise
        
        # Signal shutdown after successful delete operation
        logger.info(f"Index '{self.index_name}' deleted, shutting down IndexUpdater")
        self.shutdown_event.set()

    async def _check_bootstrap_needed(self) -> None:
        """Check if bootstrap is needed by examining first message."""
        try:
            # Fetch the first message to examine it
            messages = await self.subscription.fetch(batch=1, timeout=10)
            
            if not messages:
                logger.info(f"Index '{self.index_name}': no messages available, waiting for operations")
                return
                
            msg = messages[0]
            sequence = msg.metadata.sequence.stream
            
            if sequence == 1:
                # First message - should be CreateIndexOperation
                try:
                    operation = msgspec.msgpack.decode(msg.data, type=Operation)
                    if isinstance(operation, CreateIndexOperation):
                        logger.info(f"Index '{self.index_name}': received CreateIndexOperation at sequence 1, processing")
                        await self._apply_create_index()
                        await msg.ack()
                    else:
                        logger.error(f"Index '{self.index_name}': sequence 1 is not CreateIndexOperation: {type(operation)}")
                        await msg.nak()
                        raise RuntimeError(f"Sequence 1 should be CreateIndexOperation, got: {type(operation)}")
                except Exception as e:
                    await msg.nak()
                    raise
            else:
                # Not sequence 1 - need bootstrap
                logger.error(f"Index '{self.index_name}': first message is sequence {sequence}, bootstrap required")
                await msg.nak()
                
                # Find bootstrap source using scatter-gather
                bootstrap_source = await self._find_bootstrap_source()
                if bootstrap_source:
                    raise RuntimeError(f"Bootstrap required - first message sequence: {sequence}. Bootstrap source: {bootstrap_source}")
                else:
                    raise RuntimeError(f"Bootstrap required - first message sequence: {sequence}. No bootstrap source found")
                
        except Exception as e:
            logger.error(f"Failed to check bootstrap status for '{self.index_name}': {e}")
            raise

    async def _apply_create_index(self) -> None:
        """Apply a CreateIndex operation to fpindex - idempotent operation."""
        logger.info(f"Applying CreateIndex operation for '{self.index_name}'")
        
        url = f"{self.fpindex_url}/{self.index_name}"
        try:
            async with self.http_session.put(url) as response:
                if response.status == 200:
                    logger.info(f"Successfully created index '{self.index_name}'")
                elif response.status == 409:
                    logger.info(f"Index '{self.index_name}' already exists (idempotent)")
                else:
                    response_text = await response.text()
                    logger.error(f"Failed to create index '{self.index_name}': {response.status} {response_text}")
                    raise RuntimeError(f"HTTP {response.status}: {response_text}")
        except Exception:
            logger.exception("Error creating index %s", self.index_name)
            raise

    async def _find_bootstrap_source(self) -> str | None:
        """Find the best bootstrap source for this index."""
        return await self.manager.find_bootstrap_source(self.index_name)


logger = logging.getLogger(__name__)


class WrongLastSequence(Exception):
    pass


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

        self.indexes: dict[str, IndexStatusUpdate] = {}
        self.indexes_lock = asyncio.Lock()

        # Index discovery subscription
        self.discovery_subscription: nats.js.JetStreamContext.PushSubscription | None = None

        # Per-index updaters
        self.index_updaters: dict[str, IndexUpdater] = {}
        self.index_updaters_lock = asyncio.Lock()
        
        # Bootstrap query subscription
        self.bootstrap_subscription: nats.aio.client.Subscription | None = None

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
        
        # Start bootstrap query subscription
        await manager._start_bootstrap_subscription()

        logger.info("IndexManager ready for per-index stream management")

        return manager

    async def cleanup(self) -> None:
        """Clean up resources."""
        # Stop discovery subscription
        if self.discovery_subscription:
            await self.discovery_subscription.unsubscribe()
            
        # Stop bootstrap subscription
        if self.bootstrap_subscription:
            await self.bootstrap_subscription.unsubscribe()

        # Stop all index updaters in parallel
        async with self.index_updaters_lock:
            # Create stop tasks for all updaters
            stop_tasks = []
            for index_name, updater in self.index_updaters.items():
                task = asyncio.create_task(self._stop_updater_with_logging(index_name, updater))
                stop_tasks.append(task)

            # Wait for all stops to complete
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        await self.http_session.close()

    async def _stop_updater_with_logging(self, index_name: str, updater: IndexUpdater) -> None:
        """Stop a single updater with error logging."""
        try:
            await updater.stop()
            logger.debug(f"Stopped updater for index '{index_name}' during cleanup")
        except Exception as e:
            logger.error(f"Error stopping updater for index '{index_name}' during cleanup: {e}")

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

        config = StreamConfig(
            name=stream_name,
            subjects=[subject],
            retention=RetentionPolicy.LIMITS,
            max_msgs=1000000,  # Keep up to 1M messages
            max_age=(7 * 24 * 3600),  # Keep messages for 7 days
            storage=nats.js.api.StorageType.FILE,
            duplicate_window=300,  # 5 minutes duplicate detection window
        )

        try:
            stream_info = await self.js.add_stream(config)
            if stream_info.did_create:
                logger.info(f"Stream '{stream_name}' created successfully")
            else:
                logger.debug(f"Stream '{stream_name}' already exists with matching config")
        except nats.js.errors.BadRequestError as exc:
            if exc.err_code == 10058:  # Stream name already in use with different config
                logger.warning(f"Stream '{stream_name}' exists with different config: {exc.description}")
                # For now, just log and continue - could add config validation later
            else:
                raise

    async def _ensure_discovery_stream_exists(self) -> None:
        """Create the global discovery stream if it doesn't exist."""
        config = StreamConfig(
            name=self.discovery_stream_name,
            subjects=[self.discovery_subject_pattern],
            retention=RetentionPolicy.LIMITS,
            max_msgs_per_subject=1,  # Keep only latest event per subject (per index)
            storage=nats.js.api.StorageType.FILE,
        )

        try:
            stream_info = await self.js.add_stream(config)
            if stream_info.did_create:
                logger.info(f"Discovery stream '{self.discovery_stream_name}' created successfully")
            else:
                logger.debug(f"Discovery stream '{self.discovery_stream_name}' already exists with matching config")
        except nats.js.errors.BadRequestError as exc:
            if exc.err_code == 10058:  # Stream name already in use with different config
                logger.warning(f"Discovery stream '{self.discovery_stream_name}' exists with different config: {exc.description}")
                # For now, just log and continue - could add config validation later
            else:
                raise

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
            status = msgspec.msgpack.decode(msg.data, type=IndexStatus)

            # Build the complete status update
            status_update = IndexStatusUpdate(
                status=status,
                sequence=msg.metadata.sequence.stream,
                timestamp=msg.metadata.timestamp,
            )

            # Update integrated state cache
            async with self.indexes_lock:
                self.indexes[index_name] = status_update

            logger.debug(
                "Updated status for index %s: active=%s sequence=%s",
                index_name,
                status.active,
                status_update.sequence,
            )

            if status.active and status.pending_change is None:
                # Start subscription when index becomes active
                await self._start_index_subscription(index_name)

    async def _start_index_subscription(self, index_name: str) -> None:
        """Start an IndexUpdater for an index's operation stream."""
        async with self.index_updaters_lock:
            # Check if already started
            if index_name in self.index_updaters:
                logger.debug(f"Already have updater for index '{index_name}' operations")
                return

            stream_name = self._get_stream_name(index_name)
            subject = self._get_subject(index_name)

            # Create and start IndexUpdater while holding the lock
            updater = IndexUpdater(
                index_name=index_name,
                js=self.js,
                http_session=self.http_session,
                stream_name=stream_name,
                subject=subject,
                fpindex_url=self.fpindex_url,
                instance_name=self.instance_name,
                manager=self,
            )

            try:
                await updater.start()
                self.index_updaters[index_name] = updater
                
                # Add callback to remove updater when task completes
                if updater.pull_task:
                    def cleanup_callback(task):
                        # Only cleanup if this is still the current updater and task
                        current_updater = self.index_updaters.get(index_name)
                        if current_updater and current_updater.pull_task == task:
                            del self.index_updaters[index_name]
                            logger.info(f"Cleaned up IndexUpdater for '{index_name}' after task completion")
                    
                    updater.pull_task.add_done_callback(cleanup_callback)
                
                logger.info(f"Started IndexUpdater for index '{index_name}' operations")
            except Exception:
                # Make sure to clean up if start fails
                try:
                    await updater.stop()
                except Exception:
                    pass  # Ignore cleanup errors
                raise

    async def get_index_status(self, index_name: str) -> IndexStatusUpdate:
        """Get the current state of an index from the in-memory cache."""
        async with self.indexes_lock:
            return self.indexes.get(index_name, DEFAULT_INDEX_STATUS_UPDATE)

    async def publish_create_index(self, index_name: str) -> None:
        """
        Create index by ensuring stream exists and publishing CreateIndexOperation.
        """

        async with IndexStateChange(self, index_name, active=True) as txn:
            # Check if we need to do anything at all
            if txn.current_status.active:
                logger.info("Index %s already exists", index_name)
                return

            # Ensure stream exists for this index
            await self._ensure_stream_exists(index_name)

            # Publish the CreateIndexOperation as the first message (sequence=1)
            await self._publish_operation(index_name, CreateIndexOperation())

    async def publish_delete_index(self, index_name: str) -> None:
        """
        Publish a DeleteIndex operation with state validation and optimistic locking.
        Returns (success, message, current_state).
        """

        async with IndexStateChange(self, index_name, active=False) as txn:
            # Check if we need to do anything at all
            if not txn.current_status.active:
                logger.info("Index %s is already deleted", index_name)
                return

            # Ensure stream exists for this index
            await self._ensure_stream_exists(index_name)

            # Publish the operation to the index stream
            await self._publish_operation(index_name, DeleteIndexOperation())

    async def _publish_operation(self, index_name: str, op: Operation) -> None:
        """Publish a new operation to the index-specific stream."""
        subject = self._get_subject(index_name)
        data = msgspec.msgpack.encode(op)

        # Add deduplication header for Create/Delete operations
        headers = {}
        if isinstance(op, CreateIndexOperation):
            headers[Header.MSG_ID] = f"create-{index_name}"
        elif isinstance(op, DeleteIndexOperation):
            headers[Header.MSG_ID] = f"delete-{index_name}"

        try:
            ack = await self.js.publish(subject, data, headers=headers)
            logger.debug(f"Published operation to {subject}, seq: {ack.seq}")
        except Exception as e:
            logger.error(f"Error publishing operation to {subject}: {e}")
            raise

    async def _publish_index_status(self, index_name: str, status: IndexStatus, last_sequence: int) -> int:
        """Publish a discovery event to the global discovery stream with optimistic locking."""
        subject = self._get_discovery_subject(index_name)
        data = msgspec.msgpack.encode(status)

        headers = {
            Header.EXPECTED_STREAM: self.discovery_stream_name,
            Header.EXPECTED_LAST_SUBJECT_SEQUENCE: str(last_sequence),
        }

        try:
            ack = await self.js.publish(subject, data, headers=headers)
        except nats.js.errors.BadRequestError as exc:
            if exc.err_code == 10071:
                raise WrongLastSequence() from exc
            raise
        else:
            logger.debug(f"Published discovery event to {subject}, seq: {ack.seq}")
            return ack.seq

    async def _start_bootstrap_subscription(self) -> None:
        """Start subscription to bootstrap queries."""
        try:
            subject = f"{self.stream_prefix}.bootstrap.query.*"
            self.bootstrap_subscription = await self.nc.subscribe(
                subject, cb=self._handle_bootstrap_query
            )
            logger.info(f"Started bootstrap query subscription to {subject}")
        except Exception as e:
            logger.error(f"Error starting bootstrap subscription: {e}")
            raise

    async def _handle_bootstrap_query(self, msg: nats.aio.msg.Msg) -> None:
        """Handle bootstrap query and reply with local index status."""
        try:
            # Extract index name from subject: {prefix}.bootstrap.query.{index_name}  
            subject_parts = msg.subject.split(".")
            if len(subject_parts) < 4 or subject_parts[-2] != "query":
                logger.warning(f"Invalid bootstrap query subject: {msg.subject}")
                return
                
            index_name = subject_parts[-1]
            
            # Decode the query
            query = msgspec.msgpack.decode(msg.data, type=BootstrapQuery)
            
            # Don't reply to our own queries
            if query.requester_instance == self.instance_name:
                return
                
            logger.debug(f"Received bootstrap query for index '{index_name}' from {query.requester_instance}")
            
            # Check if we have this index and get its status
            reply = await self._get_bootstrap_reply(index_name)
            
            # Only send reply if we have the index
            if reply and msg.reply:
                reply_data = msgspec.msgpack.encode(reply)
                await self.nc.publish(msg.reply, reply_data)
                logger.debug(f"Sent bootstrap reply for index '{index_name}' to {msg.reply}")
            elif not reply:
                logger.debug(f"Not responding to bootstrap query for index '{index_name}' - don't have it")
                
        except Exception as e:
            logger.error(f"Error handling bootstrap query: {e}")

    async def _get_bootstrap_reply(self, index_name: str) -> BootstrapReply | None:
        """Generate bootstrap reply with local index status, or None if we don't have the index."""
        # TODO: Add logic to check actual fpindex status via HTTP API
        # For now, check based on whether we have an active updater
        
        async with self.index_updaters_lock:
            has_updater = index_name in self.index_updaters
            
        if not has_updater:
            return None
            
        return BootstrapReply(
            index_name=index_name,
            responder_instance=self.instance_name,
            last_sequence=0,  # TODO: Get actual last processed sequence from index
        )

    async def find_bootstrap_source(self, index_name: str, timeout: float = 5.0) -> str | None:
        """Broadcast query to find the best instance for bootstrap."""
        query_id = str(uuid.uuid4())
        reply_subject = f"{self.stream_prefix}.bootstrap.reply.{query_id}"
        query_subject = f"{self.stream_prefix}.bootstrap.query.{index_name}"
        
        # Subscribe to replies
        replies: list[BootstrapReply] = []
        
        async def collect_reply(msg: nats.aio.msg.Msg):
            try:
                reply = msgspec.msgpack.decode(msg.data, type=BootstrapReply)
                replies.append(reply)
                logger.debug(f"Received bootstrap reply from {reply.responder_instance}")
            except Exception as e:
                logger.error(f"Error processing bootstrap reply: {e}")
        
        # Subscribe to replies
        reply_subscription = await self.nc.subscribe(reply_subject, cb=collect_reply)

        try:
            # Send broadcast query
            query = BootstrapQuery(
                index_name=index_name,
                requester_instance=self.instance_name
            )
            query_data = msgspec.msgpack.encode(query)
            
            await self.nc.publish(query_subject, query_data, reply=reply_subject)
            logger.info(f"Broadcast bootstrap query for index '{index_name}' to {query_subject}")
            
            # Wait for replies
            await asyncio.sleep(timeout)
            
            # Process replies and select best source
            best_instance = self._select_best_bootstrap_source(replies)
            
            if best_instance:
                logger.info(f"Selected {best_instance} as bootstrap source for index '{index_name}'")
            else:
                logger.warning(f"No suitable bootstrap source found for index '{index_name}'")
                
            return best_instance
            
        finally:
            await reply_subscription.unsubscribe()
            
    def _select_best_bootstrap_source(self, replies: list[BootstrapReply]) -> str | None:
        """Select the best instance for bootstrap based on replies."""
        if not replies:
            return None
            
        # Sort by last_sequence (highest first) - only instances with the index respond
        replies_sorted = sorted(replies, key=lambda r: r.last_sequence, reverse=True)
        return replies_sorted[0].responder_instance


PENDING_CHANGE_TTL = datetime.timedelta(minutes=5)


class IndexStateChange:
    def __init__(self, manager: IndexManager, index_name: str, active: bool):
        self.manager = manager
        self.index_name = index_name
        self.active = active
        self.change_id = uuid.uuid4()
        self.current_status: IndexStatus
        self.current_sequence: int | None = None

    async def __aenter__(self) -> "IndexStateChange":
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        if exc is not None:
            await self.rollback()
        else:
            await self.commit()

    async def begin(self) -> None:
        retries = 0
        max_retries = 10
        base_delay = 0.01

        while True:
            if retries > 0:
                delay = base_delay * (2**retries + random.uniform(0, 1.0))
                await asyncio.sleep(delay)

            retries += 1
            if retries >= max_retries:
                raise InconsistentIndexState()

            status = await self.manager.get_index_status(self.index_name)
            self.current_status = status.status

            if status.status.pending_change is not None and status.status.pending_change.operation_id != self.change_id:
                age = datetime.datetime.now(datetime.timezone.utc) - status.timestamp
                if age < PENDING_CHANGE_TTL:
                    logger.debug(
                        "[%s] Waiting for pending change %s (age: %s) to complete",
                        self.index_name, status.status.pending_change.operation_id, age
                    )
                    continue
                else:
                    logger.warning(
                        "[%s] Found stale pending change %s (age: %s)",
                        self.index_name, status.status.pending_change.operation_id, age
                    )
                    # FIXME this should be resolved somehow
                    raise InconsistentIndexState()

            if status.status.pending_change is None and status.status.active == self.active:
                # there is no pending change, status matches, we have nothing to do
                return

            try:
                await self._publish_index_status(
                    active=self.current_status.active,
                    pending=True,
                    last_sequence=status.sequence,
                )
            except WrongLastSequence:
                continue
            else:
                break

    async def commit(self) -> None:
        if self.current_sequence is not None:
            await self._publish_index_status(
                active=self.active,
                pending=False,
                last_sequence=self.current_sequence,
            )

    async def rollback(self) -> None:
        if self.current_sequence is not None:
            await self._publish_index_status(
                active=self.current_status.active,
                pending=False,
                last_sequence=self.current_sequence,
            )

    async def _publish_index_status(self, active: bool, pending: bool, last_sequence: int) -> None:
        """Publish a discovery event to the global discovery stream with optimistic locking and retries."""
        pending_change = IndexStatusChange(operation_id=self.change_id, active=self.active) if pending else None
        status = IndexStatus(active=active, pending_change=pending_change)
        
        retries = 0
        max_retries = 5
        base_delay = 0.1
        
        while True:
            try:
                self.current_sequence = await self.manager._publish_index_status(self.index_name, status, last_sequence)
                self.current_status = status
                return
            except WrongLastSequence:
                # Sequence changed - this is expected in concurrent scenarios, not a retry case
                raise
            except Exception as e:
                retries += 1
                if retries >= max_retries:
                    logger.error(
                        "Failed to publish index status after %d retries for %s change %s: %s",
                        max_retries, self.index_name, self.change_id, e
                    )
                    raise
                
                # Exponential backoff with jitter
                delay = base_delay * (2 ** (retries - 1)) + random.uniform(0, 0.1)
                logger.warning(
                    "Failed to publish index status for %s change %s (attempt %d/%d), retrying in %.2fs: %s",
                    self.index_name, self.change_id, retries, max_retries, delay, e
                )
                await asyncio.sleep(delay)
