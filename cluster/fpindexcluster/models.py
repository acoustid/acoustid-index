#!/usr/bin/env python3

import msgspec
from typing import Union


class CreateIndexOperation(msgspec.Struct, tag="create_index"):
    """Operation to create a new index."""
    pass


class DeleteIndexOperation(msgspec.Struct, tag="delete_index"):
    """Operation to delete an index."""
    pass


Operation = Union[CreateIndexOperation, DeleteIndexOperation]