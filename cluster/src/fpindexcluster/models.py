import msgspec


class FingerprintData(msgspec.Struct):
    """Fingerprint data structure"""

    hashes: list[int] = msgspec.field(name="h")


# API Request/Response structures matching fpindex server.zig


class SearchRequest(msgspec.Struct):
    """Search request structure"""

    query: list[int] = msgspec.field(name="q")
    timeout: int = msgspec.field(name="t", default=500)
    limit: int = msgspec.field(name="l", default=40)


class SearchResultJSON(msgspec.Struct):
    """Individual search result - matches fpindex SearchResultJSON"""

    id: int = msgspec.field(name="i")
    score: int = msgspec.field(name="s")


class SearchResultsJSON(msgspec.Struct):
    """Search response structure - matches fpindex SearchResultsJSON"""

    results: list[SearchResultJSON] = msgspec.field(name="r")


# Change structures for bulk updates
class Insert(msgspec.Struct):
    """Insert operation"""

    id: int = msgspec.field(name="i")
    hashes: list[int] = msgspec.field(name="h")


class Delete(msgspec.Struct):
    """Delete operation"""

    id: int = msgspec.field(name="i")


# Tagged union for Change - exactly matches fpindex format
class ChangeInsert(msgspec.Struct):
    """Insert change - encoded as {'i': Insert}"""

    insert: Insert = msgspec.field(name="i")


class ChangeDelete(msgspec.Struct):
    """Delete change - encoded as {'d': Delete}"""

    delete: Delete = msgspec.field(name="d")


# Union type
Change = ChangeInsert | ChangeDelete


class BulkUpdateRequest(msgspec.Struct):
    """Bulk update request"""

    changes: list[Change] = msgspec.field(name="c")


class PutFingerprintRequest(msgspec.Struct):
    """Single fingerprint PUT request"""

    hashes: list[int] = msgspec.field(name="h")


class GetFingerprintResponse(msgspec.Struct):
    """GET fingerprint response - matches fpindex GetFingerprintResponse"""

    version: int = msgspec.field(name="v")


class ErrorResponse(msgspec.Struct):
    """Error response - matches fpindex ErrorResponse"""

    error: str = msgspec.field(name="e")


class EmptyResponse(msgspec.Struct):
    """Empty response - matches fpindex EmptyResponse"""

    pass


class BulkUpdateResult(msgspec.Struct):
    """Individual bulk update result"""

    id: int | None = msgspec.field(name="i", default=None)
    status: str | None = msgspec.field(name="s", default=None)
    error: str | None = msgspec.field(name="e", default=None)
    change: dict | None = msgspec.field(name="c", default=None)


class BulkUpdateResponse(msgspec.Struct):
    """Bulk update response"""

    results: list[BulkUpdateResult] = msgspec.field(name="r")


class GetIndexResponse(msgspec.Struct):
    """Get index response - matches fpindex GetIndexResponse"""

    version: int = msgspec.field(name="v")


