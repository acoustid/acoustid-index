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
