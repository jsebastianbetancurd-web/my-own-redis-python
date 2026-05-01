def parse_resp(data):
    """
    Parses RESP (Redis Serialization Protocol) data into a list of arguments.
    Supports Simple Arrays (e.g., *2\r\n$4\r\nPING\r\n).
    """
    if not data: return None, b"", 0
    if data[0:1] != b"*": return None, b"", 0
    try:
        idx = data.find(b"\r\n")
        num_args = int(data[1:idx])
        args = []
        curr_idx = idx + 2
        for _ in range(num_args):
            idx = data.find(b"\r\n", curr_idx)
            arg_len = int(data[curr_idx+1:idx])
            curr_idx = idx + 2
            arg = data[curr_idx:curr_idx+arg_len]
            args.append(arg)
            curr_idx += arg_len + 2
        return args, data[curr_idx:], curr_idx
    except (ValueError, IndexError):
        return None, b"", 0

def encode_resp_array(args):
    """
    Encodes a list of arguments into RESP array format.
    Supports strings, integers, and nested lists.
    """
    res = f"*{len(args)}\r\n".encode()
    for arg in args:
        if isinstance(arg, int):
            res += f":{arg}\r\n".encode()
        elif isinstance(arg, list):
            res += encode_resp_array(arg)
        else:
            # Handle bytes or strings
            if isinstance(arg, str):
                arg = arg.encode()
            res += b"$" + str(len(arg)).encode() + b"\r\n" + arg + b"\r\n"
    return res

def encode_stream_entry(eid, fields):
    """
    Specialized encoder for Redis Stream entries.
    """
    res = b"*2\r\n"
    res += b"$" + str(len(eid)).encode() + b"\r\n" + eid + b"\r\n"
    res += f"*{len(fields)*2}\r\n".encode()
    for f, v in fields.items():
        if isinstance(f, str): f = f.encode()
        if isinstance(v, str): v = v.encode()
        res += b"$" + str(len(f)).encode() + b"\r\n" + f + b"\r\n"
        res += b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
    return res
