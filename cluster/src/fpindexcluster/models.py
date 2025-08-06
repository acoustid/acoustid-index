import msgspec


class FingerprintData(msgspec.Struct):
    """Fingerprint data structure"""
    hashes: list[int] = msgspec.field(name="h")
