import msgpack


def _parse_header(data):
    # The snapshot is a single self-delimiting msgpack header followed by the raw
    # segment payloads. Header keys use the single-char field-name prefix convention:
    # f=format, g=generation, s=segments (each {i=info, s=size}).
    unpacker = msgpack.Unpacker(raw=False)
    unpacker.feed(data)
    header = unpacker.unpack()
    return header, unpacker.tell()


def test_snapshot_export(client, index_name, create_index):
    # A fingerprint that stays in memory (no checkpoint at the default threshold), so
    # this exercises the endpoint wiring + chunked streaming + the msgpack header. The
    # unit test covers the file-segment payloads.
    r = client.put(f"/{index_name}/1", json={"hashes": [101, 201, 301]})
    assert r.status_code == 200

    resp = client.get(f"/{index_name}/_snapshot")
    assert resp.status_code == 200

    header, consumed = _parse_header(resp.content)
    assert header["f"] == 1  # format version
    assert header["g"] >= 1  # generation
    assert isinstance(header["s"], list)  # segments (empty without a checkpoint)

    # Payload bytes = sum of segment sizes; header consumed the rest.
    payload_len = sum(seg["s"] for seg in header["s"])
    assert consumed + payload_len == len(resp.content)


def test_snapshot_missing_index(client, index_name):
    resp = client.get(f"/{index_name}/_snapshot")
    assert resp.status_code == 404
