#!/usr/bin/env python3

from fpindexcluster.server import is_valid_index_name


def test_valid_index_names() -> None:
    """Test valid index names."""
    assert is_valid_index_name("a")
    assert is_valid_index_name("a1")
    assert is_valid_index_name("a1-b")
    assert is_valid_index_name("a1_b")
    assert is_valid_index_name("Index123")
    assert is_valid_index_name("test_index")
    assert is_valid_index_name("test-index")


def test_invalid_index_names() -> None:
    """Test invalid index names."""
    # Empty string
    assert not is_valid_index_name("")

    # Start with underscore or hyphen
    assert not is_valid_index_name("_1b2")
    assert not is_valid_index_name("-1b2")

    # Invalid characters
    assert not is_valid_index_name("a/a")
    assert not is_valid_index_name(".foo")
    assert not is_valid_index_name("test.index")
    assert not is_valid_index_name("test index")  # space
    assert not is_valid_index_name("test@index")
