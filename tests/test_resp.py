import pytest
from app.main import parse_resp, encode_resp_array

def test_parse_resp_simple_array():
    data = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
    args, rest, length = parse_resp(data)
    assert args == [b"ECHO", b"hey"]
    assert rest == b""
    assert length == len(data)

def test_parse_resp_empty_data():
    args, rest, length = parse_resp(b"")
    assert args is None
    assert rest == b""
    assert length == 0

def test_parse_resp_invalid_format():
    args, rest, length = parse_resp(b"INVALID")
    assert args is None
    assert rest == b""
    assert length == 0

def test_encode_resp_array_simple():
    args = [b"SET", b"foo", b"bar"]
    encoded = encode_resp_array(args)
    assert encoded == b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"

def test_encode_resp_array_with_int():
    args = [b"SET", b"foo", 123]
    encoded = encode_resp_array(args)
    assert encoded == b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n:123\r\n"

def test_encode_resp_array_nested():
    args = [b"MGET", [b"a", b"b"]]
    encoded = encode_resp_array(args)
    assert encoded == b"*2\r\n$4\r\nMGET\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
