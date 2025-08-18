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
    timestamp=datetime.datetime.fromtimestamp(0),
)


class CreateIndexOperation(msgspec.Struct, tag="create_index"):
    """Operation to create a new index."""

    pass


class DeleteIndexOperation(msgspec.Struct, tag="delete_index"):
    """Operation to delete an index."""

    pass


Operation = Union[CreateIndexOperation, DeleteIndexOperation]
