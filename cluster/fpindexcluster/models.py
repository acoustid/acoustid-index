#!/usr/bin/env python3

import msgspec
import uuid
import datetime
from typing import Union


class IndexStatusChange(msgspec.Struct):
    operation_id: uuid.UUID
    active: bool


class IndexStatus(msgspec.Struct):
    active: bool
    pending_change: IndexStatusChange | None = None


class IndexStatusUpdate(msgspec.Struct):
    status: IndexStatus
    sequence: int
    timestamp: datetime.datetime


DEFAULT_INDEX_STATUS = IndexStatus(
    active=False,
    pending_change=None,
)


DEFAULT_INDEX_STATUS_UPDATE = IndexStatusUpdate(
    status=DEFAULT_INDEX_STATUS,
    sequence=0,
    timestamp=datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc),
)


class CreateIndexOperation(msgspec.Struct, tag="create_index"):
    """Operation to create a new index - used as stream filler to ensure sequence=1 exists."""

    pass


class DeleteIndexOperation(msgspec.Struct, tag="delete_index"):
    """Operation to delete an index."""

    pass


# Update operation structures that mirror fpindex Change types
class Insert(msgspec.Struct):
    """Insert operation - add fingerprint to index."""

    id: int  # u32
    hashes: list[int]  # []u32


class Delete(msgspec.Struct):
    """Delete operation - remove fingerprint from index."""

    id: int  # u32


class Change(msgspec.Struct, omit_defaults=True):
    """Change operation - union of insert or delete."""

    insert: Insert | None = None
    delete: Delete | None = None


class UpdateOperation(msgspec.Struct, tag="update"):
    """Operation to update fingerprints in an index."""

    changes: list[Change]
    metadata: dict[str, str] | None = None



Operation = Union[CreateIndexOperation, DeleteIndexOperation, UpdateOperation]


class BootstrapQuery(msgspec.Struct):
    """Query broadcast to find the best instance for bootstrap."""

    index_name: str
    requester_instance: str


class BootstrapReply(msgspec.Struct):
    """Reply with local index status for bootstrap selection."""

    index_name: str
    responder_instance: str
    last_sequence: int  # Highest sequence processed
