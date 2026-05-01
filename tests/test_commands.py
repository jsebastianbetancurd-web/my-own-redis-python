import pytest
from app.main import process_command, data_store

@pytest.fixture(autouse=True)
def clear_data_store():
    data_store.clear()
    yield

def test_ping():
    res = process_command(b"PING", [])
    assert res == b"+PONG\r\n"

def test_set_get():
    process_command(b"SET", [b"foo", b"bar"])
    res = process_command(b"GET", [b"foo"])
    assert res == b"$3\r\nbar\r\n"

def test_get_nonexistent():
    res = process_command(b"GET", [b"nonexistent"])
    assert res == b"$-1\r\n"

def test_incr():
    process_command(b"SET", [b"counter", b"10"])
    res = process_command(b"INCR", [b"counter"])
    assert res == b":11\r\n"
    assert data_store[b"counter"][0] == b"11"

def test_incr_new_key():
    res = process_command(b"INCR", [b"new_counter"])
    assert res == b":1\r\n"
    assert data_store[b"new_counter"][0] == b"1"

def test_echo():
    res = process_command(b"ECHO", [b"hello world"])
    assert res == b"$11\r\nhello world\r\n"
