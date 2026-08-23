import socket

import pytest


# The legacy protocol serves a single fixed "main" index shared by all
# connections, so committed docs persist across tests. Each test uses its own
# id/hash namespace so searches only ever match their own data.


class Legacy:
    """Minimal client for the legacy line protocol."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.f = self.sock.makefile("rwb")

    def cmd(self, line):
        self.f.write(line.encode("ascii") + b"\r\n")
        self.f.flush()
        return self.f.readline().decode("ascii").rstrip("\r\n")

    def close(self):
        self.f.close()
        self.sock.close()


@pytest.fixture
def legacy(server):
    c = Legacy(server.legacy_port)
    yield c
    c.close()


def test_echo(legacy):
    assert legacy.cmd("echo hello world") == "OK hello world"


def test_empty_line(legacy):
    assert legacy.cmd("") == "OK "


def test_unknown_command(legacy):
    assert legacy.cmd("frobnicate x").startswith("ERR ")


def test_quit_acknowledges_and_closes_connection(legacy):
    # v2022 accepted every command beginning with "quit", then closed.
    assert legacy.cmd("quit now") == "OK "
    assert legacy.f.readline() == b""


def test_invalid_fingerprint(legacy):
    assert legacy.cmd("search notanumber").startswith("ERR ")


def test_insert_requires_transaction(legacy):
    assert legacy.cmd("insert 1 1,2,3") == "ERR not in transaction"


def test_double_begin(legacy):
    assert legacy.cmd("begin") == "OK "
    assert legacy.cmd("begin") == "ERR already in transaction"
    legacy.cmd("rollback")


def test_insert_search_commit(legacy):
    assert legacy.cmd("begin") == "OK "
    assert legacy.cmd("insert 1001 11000,12000,13000") == "OK "
    assert legacy.cmd("insert 1002 11000,12000,19000") == "OK "
    assert legacy.cmd("commit") == "OK "

    # 1001 matches all three (score 3); 1002 matches 11000,12000 (score 2); desc.
    assert legacy.cmd("search 11000,12000,13000") == "OK 1001:3 1002:2"
    assert legacy.cmd("search 11000,12000,19000") == "OK 1002:3 1001:2"


def test_top_score_percent_rounds_cutoff_like_v2022(legacy):
    assert legacy.cmd("begin") == "OK "
    assert legacy.cmd("insert 3001 31001,31002,31003") == "OK "
    assert legacy.cmd("insert 3002 31001") == "OK "
    assert legacy.cmd("commit") == "OK "

    # v2022 rounds 3 * 50% to 2, excluding the score-1 result.
    assert legacy.cmd("set top_score_percent 50") == "OK "
    assert legacy.cmd("search 31001,31002,31003") == "OK 3001:3"


def test_rollback_discards(legacy):
    assert legacy.cmd("begin") == "OK "
    assert legacy.cmd("insert 6001 61000,62000,63000") == "OK "
    assert legacy.cmd("rollback") == "OK "
    assert legacy.cmd("search 61000,62000,63000") == "OK "  # nothing committed


def test_session_attribute_limits_results(legacy):
    legacy.cmd("begin")
    legacy.cmd("insert 2001 21000,22000")
    legacy.cmd("insert 2002 21000,22000")
    legacy.cmd("commit")

    both = legacy.cmd("search 21000,22000")
    assert both.startswith("OK ") and len(both[3:].split()) == 2

    assert legacy.cmd("set max_results 1") == "OK "
    assert legacy.cmd("get max_results") == "OK 1"
    one = legacy.cmd("search 21000,22000")
    assert len(one[3:].split()) == 1


def test_index_attribute_persists(legacy):
    # index attributes require a transaction; missing ones read empty
    assert legacy.cmd("get attribute leg_attr") == "OK "
    assert legacy.cmd("set attribute leg_attr 123") == "ERR not in transaction"

    assert legacy.cmd("begin") == "OK "
    assert legacy.cmd("set attribute leg_attr 123") == "OK "
    assert legacy.cmd("commit") == "OK "
    assert legacy.cmd("get leg_attr") == "OK 123"


# --- which index the legacy port serves -------------------------------------
#
# The name was fixed at "main". It has to be selectable to put the legacy port
# in front of data that already exists under another name -- production serves
# "acoustid" -- because pointing it at the wrong index does not fail. It creates
# an empty one and answers every search with a miss, which looks like a working
# server holding no data.


@pytest.fixture(scope="module")
def named_server(_built):
    from conftest import Server

    srv = Server(legacy_index_name="acoustid")
    srv.start()
    try:
        yield srv
    finally:
        srv.cleanup()


def test_legacy_writes_land_in_the_named_index(named_server, session):
    """Committed over the legacy port, read back over HTTP under that name."""
    c = Legacy(named_server.legacy_port)
    try:
        assert c.cmd("begin") == "OK "
        assert c.cmd("insert 7001 21000,22000,23000") == "OK "
        assert c.cmd("commit") == "OK "
    finally:
        c.close()

    r = session.get(f"{named_server.get_url()}/acoustid")
    assert r.status_code == 200, r.text
    assert r.json()["stats"]["num_docs"] == 1

    # And nothing was created under the old fixed name.
    assert session.get(f"{named_server.get_url()}/main").status_code == 404


def test_legacy_search_reads_the_named_index(named_server, session):
    c = Legacy(named_server.legacy_port)
    try:
        assert c.cmd("begin") == "OK "
        assert c.cmd("insert 7002 31000,32000,33000") == "OK "
        assert c.cmd("commit") == "OK "
        assert c.cmd("search 31000,32000,33000") == "OK 7002:3"
    finally:
        c.close()


def test_the_default_is_still_main(server, session):
    """The unflagged server is what every existing deployment runs."""
    assert session.get(f"{server.get_url()}/main").status_code == 200


def test_max_document_id_reflects_the_index(legacy):
    """`get attribute max_document_id` must answer from index state.

    The old C++ index derived this: IndexWriter tracked the highest id it was
    given and persisted it on commit. Nothing writes that metadata key here, and
    a replica fed by the changelog never goes through a writer that could, so
    answering from metadata alone returns empty.

    Empty is not a harmless "unknown". acoustid-server reads it as
    `int(... or "0")` and uses it as the lower bound of the fallback scan in
    FingerprintSearcher, so zero turns a bounded tail scan into a full scan of
    the fingerprint table.
    """
    # The index is shared across tests in this file, so this asserts on change
    # rather than on absolute values -- but it must be a NUMBER, never empty,
    # which is the whole bug.
    before = legacy.cmd("get attribute max_document_id")
    assert before.startswith("OK ")
    assert before[3:].strip().isdigit(), before

    high = 900456
    assert legacy.cmd("begin") == "OK "
    assert legacy.cmd("insert 900123 901,902,903") == "OK "
    assert legacy.cmd(f"insert {high} 904,905,906") == "OK "
    assert legacy.cmd("commit") == "OK "

    assert legacy.cmd("get attribute max_document_id") == f"OK {high}"

    # Highest ever, not most recent: inserting a lower id must not lower it.
    assert legacy.cmd("begin") == "OK "
    assert legacy.cmd("insert 900200 907,908,909") == "OK "
    assert legacy.cmd("commit") == "OK "
    assert legacy.cmd("get attribute max_document_id") == f"OK {high}"

    # A real metadata attribute still comes from metadata.
    assert legacy.cmd("begin") == "OK "
    assert legacy.cmd("set attribute some_other_attr 42") == "OK "
    assert legacy.cmd("commit") == "OK "
    assert legacy.cmd("get attribute some_other_attr") == "OK 42"
