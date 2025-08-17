#!/usr/bin/env python3

import msgspec
from typing import Union, Optional
from enum import Enum


class IndexState(Enum):
    """Index lifecycle states."""

    NOT_EXISTS = "not_exists"
    CREATING = "creating"
    ACTIVE = "active"
    DELETING = "deleting"
    DELETED = "deleted"

    def exists(self) -> bool:
        """Check if the index exists (is not NOT_EXISTS, DELETING or DELETED)."""
        return self not in (
            IndexState.NOT_EXISTS,
            IndexState.DELETING,
            IndexState.DELETED,
        )


class CreateIndexOperation(msgspec.Struct, tag="create_index"):
    """Operation to create a new index."""

    pass


class DeleteIndexOperation(msgspec.Struct, tag="delete_index"):
    """Operation to delete an index."""

    pass


class IndexCreatingEvent(msgspec.Struct, tag="index_creating"):
    """Event published when index creation starts."""

    operation_id: Optional[str] = None


class IndexCreatedEvent(msgspec.Struct, tag="index_created"):
    """Event published when a new index is created."""

    operation_id: Optional[str] = None


class IndexDeletingEvent(msgspec.Struct, tag="index_deleting"):
    """Event published when index deletion starts."""

    operation_id: Optional[str] = None


class IndexDeletedEvent(msgspec.Struct, tag="index_deleted"):
    """Event published when an index is deleted."""

    operation_id: Optional[str] = None


Operation = Union[CreateIndexOperation, DeleteIndexOperation]
DiscoveryEvent = Union[
    IndexCreatingEvent, IndexCreatedEvent, IndexDeletingEvent, IndexDeletedEvent
]


def get_index_state(event: DiscoveryEvent | None) -> IndexState:
    """Get the current index state from the latest discovery event."""
    if event is None:
        return IndexState.NOT_EXISTS
    elif isinstance(event, IndexCreatingEvent):
        return IndexState.CREATING
    elif isinstance(event, IndexCreatedEvent):
        return IndexState.ACTIVE
    elif isinstance(event, IndexDeletingEvent):
        return IndexState.DELETING
    elif isinstance(event, IndexDeletedEvent):
        return IndexState.DELETED
    else:
        return IndexState.NOT_EXISTS
