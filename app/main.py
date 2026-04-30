import socket
import threading
import time
import argparse
import os
import struct
import sys
import bisect
import math

# Global configuration
config = {
    "role": "master",
    "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
    "master_repl_offset": 0,
    "master_host": None,
    "master_port": None,
    "port": 6379,
    "dir": ".",
    "dbfilename": "dump.rdb",
    "appendonly": "no",
    "appenddirname": "appendonlydir",
    "appendfilename": "appendonly.aof",
    "appendfsync": "everysec"
}

# Replicas tracking
replicas = []
replicas_lock = threading.Lock()
replicas_condition = threading.Condition(replicas_lock)

# Pub/Sub tracking
pubsub_subscriptions = {}
pubsub_lock = threading.Lock()

# AOF state
aof_handle = None
aof_lock = threading.Lock()

# In-memory database
data_store = {}
key_versions = {}
data_condition = threading.Condition()

def mark_modified(key):
    key_versions[key] = key_versions.get(key, 0) + 1

def spread_bits(x):
    x = (x | (x << 16)) & 0x0000FFFF0000FFFF
    x = (x | (x << 8))  & 0x00FF00FF00FF00FF
    x = (x | (x << 4))  & 0x0F0F0F0F0F0F0F0F
    x = (x | (x << 2))  & 0x3333333333333333
    x = (x | (x << 1))  & 0x5555555555555555
    return x

def geohash_encode(lon, lat):
    lon_min, lon_max = -180, 180
    lat_min, lat_max = -85.05112878, 85.05112878
    
    # Normalize and truncate to 26 bits
    lon_int = int(((lon - lon_min) / (lon_max - lon_min)) * (2**26))
    lat_int = int(((lat - lat_min) / (lat_max - lat_min)) * (2**26))
    
    # Clamp to max 26-bit value
    lon_int = min(max(lon_int, 0), (2**26) - 1)
    lat_int = min(max(lat_int, 0), (2**26) - 1)
    
    # Interleave
    return (spread_bits(lon_int) << 1) | spread_bits(lat_int)

def squish_bits(x):
    x &= 0x5555555555555555
    x = (x | (x >> 1)) & 0x3333333333333333
    x = (x | (x >> 2)) & 0x0F0F0F0F0F0F0F0F
    x = (x | (x >> 4)) & 0x00FF00FF00FF00FF
    x = (x | (x >> 8)) & 0x0000FFFF0000FFFF
    x = (x | (x >> 16)) & 0x00000000FFFFFFFF
    return x

def geohash_decode(score):
    lon_int = squish_bits(score >> 1)
    lat_int = squish_bits(score)
    
    lon_min, lon_max = -180.0, 180.0
    lat_min, lat_max = -85.05112878, 85.05112878
    
    lon = lon_min + (lon_int + 0.5) * (lon_max - lon_min) / (1 << 26)
    lat = lat_min + (lat_int + 0.5) * (lat_max - lat_min) / (1 << 26)
    
    return lon, lat

def haversine_distance(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6372797.560856
    return c * r

class RedisSortedSet:
    def __init__(self):
        self.members = {} 
        self.sorted_members = [] 

    def add(self, member, score):
        is_new = member not in self.members
        if not is_new:
            old_score = self.members[member]
            self.sorted_members.remove((old_score, member))
        
        self.members[member] = score
        bisect.insort(self.sorted_members, (score, member))
        return 1 if is_new else 0

    def rank(self, member):
        if member not in self.members:
            return None
        score = self.members[member]
        idx = bisect.bisect_left(self.sorted_members, (score, member))
        return idx

    def get_range(self, start, stop):
        n = len(self.sorted_members)
        if n == 0: return []
        if start < 0:
            start = n + start
            if start < 0: start = 0
        if stop < 0:
            stop = n + stop
            if stop < 0: stop = 0
        if start >= n: return []
        if stop >= n: stop = n - 1
        if start > stop: return []
        return [m[1] for m in self.sorted_members[start : stop + 1]]

    def rem(self, member):
        if member in self.members:
            score = self.members.pop(member)
            self.sorted_members.remove((score, member))
            return 1
        return 0

class RedisStream:
    def __init__(self):
        self.entries = [] 
        self.last_id = (0, 0) 
    
    def parse_id(self, id_bytes, is_end=False):
        if id_bytes == b"-": return (0, 0)
        if id_bytes == b"+": return (float('inf'), float('inf'))
        try:
            if b"-" in id_bytes:
                t_part, s_part = id_bytes.split(b"-")
                return (int(t_part), int(s_part))
            else:
                t = int(id_bytes)
                s = float('inf') if is_end else 0
                return (t, s)
        except ValueError: return (0, 0)

    def validate_and_generate_id(self, entry_id_str):
        if entry_id_str == b"*":
            t = int(time.time() * 1000)
            last_t, last_s = self.last_id
            s = last_s + 1 if t == last_t else 0
            final_id = f"{t}-{s}".encode()
            return True, (t, s, final_id)
        try:
            t_part, s_part = entry_id_str.split(b"-")
            t = int(t_part)
        except ValueError: return False, "Invalid ID format"
        last_t, last_s = self.last_id
        if s_part == b"*":
            if t == 0: s = last_s + 1 if last_t == 0 else 1
            elif t == last_t: s = last_s + 1
            elif t > last_t: s = 0
            else: return False, "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        else:
            try: s = int(s_part)
            except ValueError: return False, "Invalid ID format"
            if t == 0 and s == 0: return False, "ERR The ID specified in XADD must be greater than 0-0"
            if t < last_t or (t == last_t and s <= last_s): return False, "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        final_id = f"{t}-{s}".encode()
        return True, (t, s, final_id)

    def add_entry(self, entry_id_str, fields):
        success, result = self.validate_and_generate_id(entry_id_str)
        if not success: return False, result
        t, s, final_id = result
        self.last_id = (t, s)
        self.entries.append((final_id, fields))
        return True, final_id

    def get_range(self, start_id_bytes, end_id_bytes):
        st, ss = self.parse_id(start_id_bytes, is_end=False)
        et, es = self.parse_id(end_id_bytes, is_end=True)
        results = []
        for eid, fields in self.entries:
            t, s = self.parse_id(eid)
            if (t, s) >= (st, ss) and (t, s) <= (et, es): results.append((eid, fields))
        return results

    def get_after(self, start_id_bytes):
        st, ss = self.parse_id(start_id_bytes)
        results = []
        for eid, fields in self.entries:
            t, s = self.parse_id(eid)
            if (t, s) > (st, ss): results.append((eid, fields))
        return results

def encode_stream_entry(eid, fields):
    res = b"*2\r\n"
    res += b"$" + str(len(eid)).encode() + b"\r\n" + eid + b"\r\n"
    res += f"*{len(fields)*2}\r\n".encode()
    for f, v in fields.items():
        res += b"$" + str(len(f)).encode() + b"\r\n" + f + b"\r\n"
        res += b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
    return res

def encode_resp_array(args):
    res = f"*{len(args)}\r\n".encode()
    for arg in args:
        if isinstance(arg, int):
            res += f":{arg}\r\n".encode()
        else:
            res += b"$" + str(len(arg)).encode() + b"\r\n" + arg + b"\r\n"
    return res

def is_write_command(cmd):
    return cmd.upper() in [b"SET", b"INCR", b"XADD", b"RPUSH", b"LPUSH", b"LPOP", b"ZADD", b"ZREM"]

def propagate(cmd, args):
    resp_cmd = encode_resp_array([cmd] + list(args))
    config["master_repl_offset"] += len(resp_cmd)
    with replicas_lock:
        for replica_info in replicas:
            try:
                replica_info["conn"].send(resp_cmd)
            except:
                pass

def write_to_aof(args):
    global aof_handle
    if not aof_handle: return
    data = encode_resp_array(args)
    with aof_lock:
        aof_handle.write(data)
        if config["appendfsync"] == "always":
            aof_handle.flush()
            os.fsync(aof_handle.fileno())

def read_length(f):
    b = f.read(1)
    if not b: return None
    b = b[0]
    enc_type = (b & 0xC0) >> 6
    if enc_type == 0:
        return b & 0x3F
    elif enc_type == 1:
        next_b = f.read(1)[0]
        return ((b & 0x3F) << 8) | next_b
    elif enc_type == 2:
        return struct.unpack(">I", f.read(4))[0]
    elif enc_type == 3:
        return (b & 0x3F, True)
    return None

def read_string(f):
    res = read_length(f)
    if res is None: return None
    if isinstance(res, tuple):
        spec_type, is_special = res
        if spec_type == 0: 
            val = f.read(1)[0]
            return str(val).encode()
        elif spec_type == 1: 
            val = struct.unpack("<H", f.read(2))[0]
            return str(val).encode()
        elif spec_type == 2: 
            val = struct.unpack("<I", f.read(4))[0]
            return str(val).encode()
        return None
    return f.read(res)

def load_rdb():
    path = os.path.join(config["dir"], config["dbfilename"])
    if not os.path.exists(path): return
    
    with open(path, "rb") as f:
        magic = f.read(5)
        if magic != b"REDIS": return
        f.read(4) 
        
        while True:
            b = f.read(1)
            if not b or b == b"\xFF": break 
            
            if b == b"\xFA": 
                read_string(f) 
                read_string(f) 
                continue
            
            if b == b"\xFE": 
                read_length(f) 
                continue
            
            if b == b"\xFB": 
                read_length(f) 
                read_length(f) 
                continue
            
            expiry = None
            if b == b"\xFC": 
                expiry = struct.unpack("<Q", f.read(8))[0] / 1000.0
                value_type = f.read(1)[0]
            elif b == b"\xFD": 
                expiry = struct.unpack("<I", f.read(4))[0]
                value_type = f.read(1)[0]
            else:
                value_type = b[0]
            
            key = read_string(f)
            value = read_string(f) 
            
            if key and value:
                with data_condition:
                    data_store[key] = (value, expiry)

def process_command(cmd, args):
    cmd = cmd.upper()
    if cmd == b"PING":
        return b"+PONG\r\n"
    elif cmd == b"REPLCONF":
        return b"+OK\r\n"
    elif cmd == b"PSYNC":
        res_str = f"FULLRESYNC {config['master_replid']} {config['master_repl_offset']}\r\n"
        rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000ff8c6e06255a95cb7d"
        rdb_bytes = bytes.fromhex(rdb_hex)
        res_bytes = b"+" + res_str.encode()
        res_bytes += b"$" + str(len(rdb_bytes)).encode() + b"\r\n" + rdb_bytes
        return res_bytes
    elif cmd == b"WAIT":
        return b":0\r\n"
    elif cmd == b"CONFIG":
        if len(args) >= 2 and args[0].upper() == b"GET":
            param = args[1].decode().lower()
            val = config.get(param, "")
            return encode_resp_array([param.encode(), str(val).encode()])
        return b"-ERR syntax error\r\n"
    elif cmd == b"ACL":
        if args and args[0].upper() == b"WHOAMI":
            return b"$7\r\ndefault\r\n"
        return b"-ERR unknown command\r\n"
    elif cmd == b"KEYS":
        pattern = args[0]
        if pattern == b"*":
            with data_condition:
                res = encode_resp_array(list(data_store.keys()))
                return res
        return encode_resp_array([])
    elif cmd == b"PUBLISH":
        if len(args) < 2: return b"-ERR wrong number of arguments for 'publish' command\r\n"
        channel = args[0]
        with pubsub_lock:
            subs = pubsub_subscriptions.get(channel, set())
            count = len(subs)
        return f":{count}\r\n".encode()
    elif cmd == b"ZADD":
        if len(args) < 3: return b"-ERR wrong number of arguments for 'zadd' command\r\n"
        key, score, member = args[0], float(args[1]), args[2]
        with data_condition:
            if key not in data_store:
                data_store[key] = (RedisSortedSet(), None)
            zset, expiry = data_store[key]
            if not isinstance(zset, RedisSortedSet):
                return b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            added = zset.add(member, score)
            mark_modified(key)
        return f":{added}\r\n".encode()
    elif cmd == b"ZREM":
        if len(args) < 2: return b"-ERR wrong number of arguments for 'zrem' command\r\n"
        key, member = args[0], args[1]
        with data_condition:
            entry = data_store.get(key)
            if not entry or not isinstance(entry[0], RedisSortedSet):
                return b":0\r\n"
            zset = entry[0]
            removed = zset.rem(member)
            if removed:
                mark_modified(key)
        return f":{removed}\r\n".encode()
    elif cmd == b"ZCARD":
        if not args: return b"-ERR wrong number of arguments for 'zcard' command\r\n"
        key = args[0]
        entry = data_store.get(key)
        if not entry or not isinstance(entry[0], RedisSortedSet):
            return b":0\r\n"
        zset = entry[0]
        return f":{len(zset.members)}\r\n".encode()
    elif cmd == b"ZRANK":
        if len(args) < 2: return b"-ERR wrong number of arguments for 'zrank' command\r\n"
        key, member = args[0], args[1]
        entry = data_store.get(key)
        if not entry or not isinstance(entry[0], RedisSortedSet):
            return b"$-1\r\n"
        zset = entry[0]
        rank = zset.rank(member)
        if rank is None:
            return b"$-1\r\n"
        return f":{rank}\r\n".encode()
    elif cmd == b"ZSCORE":
        if len(args) < 2: return b"-ERR wrong number of arguments for 'zscore' command\r\n"
        key, member = args[0], args[1]
        entry = data_store.get(key)
        if not entry or not isinstance(entry[0], RedisSortedSet):
            return b"$-1\r\n"
        zset = entry[0]
        if member not in zset.members:
            return b"$-1\r\n"
        score = zset.members[member]
        score_str = str(score)
        if score_str.endswith(".0"):
            score_str = score_str[:-2]
        return b"$" + str(len(score_str)).encode() + b"\r\n" + score_str.encode() + b"\r\n"
    elif cmd == b"ZRANGE":
        if len(args) < 3: return b"-ERR wrong number of arguments for 'zrange' command\r\n"
        key, start, stop = args[0], int(args[1]), int(args[2])
        entry = data_store.get(key)
        if not entry or not isinstance(entry[0], RedisSortedSet):
            return b"*0\r\n"
        zset = entry[0]
        members = zset.get_range(start, stop)
        return encode_resp_array(members)
    elif cmd == b"GEOADD":
        if len(args) < 4: return b"-ERR wrong number of arguments for 'geoadd' command\r\n"
        key = args[0]
        locations = []
        try:
            for i in range(1, len(args), 3):
                if i + 2 >= len(args): break
                lon = float(args[i])
                lat = float(args[i+1])
                member = args[i+2]
                
                if not (-180 <= lon <= 180):
                    return f"-ERR invalid longitude {lon}\r\n".encode()
                if not (-85.05112878 <= lat <= 85.05112878):
                    return f"-ERR invalid latitude {lat}\r\n".encode()
                
                locations.append((member, lon, lat))
        except ValueError:
            return b"-ERR value is not a valid float\r\n"
        
        added_count = 0
        with data_condition:
            if key not in data_store:
                data_store[key] = (RedisSortedSet(), None)
            zset, expiry = data_store[key]
            if not isinstance(zset, RedisSortedSet):
                return b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            
            for member, lon, lat in locations:
                score = geohash_encode(lon, lat)
                added_count += zset.add(member, score)
            
            if added_count > 0:
                mark_modified(key)
                
        return f":{added_count}\r\n".encode()
    elif cmd == b"GEOPOS":
        if len(args) < 2: return b"-ERR wrong number of arguments for 'geopos' command\r\n"
        key = args[0]
        members = args[1:]
        entry = data_store.get(key)
        
        res = f"*{len(members)}\r\n".encode()
        if not entry or not isinstance(entry[0], RedisSortedSet):
            for _ in members:
                res += b"*-1\r\n"
            return res
            
        zset = entry[0]
        for member in members:
            if member in zset.members:
                score = zset.members[member]
                lon, lat = geohash_decode(int(score))
                lon_str = str(lon).encode()
                lat_str = str(lat).encode()
                res += b"*2\r\n$" + str(len(lon_str)).encode() + b"\r\n" + lon_str + b"\r\n"
                res += b"$" + str(len(lat_str)).encode() + b"\r\n" + lat_str + b"\r\n"
            else:
                res += b"*-1\r\n"
        return res
    elif cmd == b"GEODIST":
        if len(args) < 3: return b"-ERR wrong number of arguments for 'geodist' command\r\n"
        key = args[0]
        member1 = args[1]
        member2 = args[2]
        entry = data_store.get(key)
        if not entry or not isinstance(entry[0], RedisSortedSet):
            return b"$-1\r\n"
        zset = entry[0]
        if member1 not in zset.members or member2 not in zset.members:
            return b"$-1\r\n"
        
        score1 = zset.members[member1]
        score2 = zset.members[member2]
        
        lon1, lat1 = geohash_decode(int(score1))
        lon2, lat2 = geohash_decode(int(score2))
        
        dist = haversine_distance(lon1, lat1, lon2, lat2)
        dist_str = f"{dist:.4f}".encode()
        return b"$" + str(len(dist_str)).encode() + b"\r\n" + dist_str + b"\r\n"
    elif cmd == b"GEOSEARCH":
        if len(args) < 7: return b"-ERR wrong number of arguments for 'geosearch' command\r\n"
        key = args[0]
        # Expected: GEOSEARCH key FROMLONLAT <lon> <lat> BYRADIUS <radius> <unit>
        try:
            # Basic parsing as per requirement
            center_lon = float(args[2])
            center_lat = float(args[3])
            radius = float(args[5])
            unit = args[6].lower()
        except (ValueError, IndexError):
            return b"-ERR value is not a valid float\r\n"
        
        # Unit conversion to meters
        # m: meters, km: kilometers, mi: miles, ft: feet
        conv = {b"m": 1.0, b"km": 1000.0, b"mi": 1609.34, b"ft": 0.3048}
        if unit not in conv:
            return b"-ERR unsupported unit\r\n"
        radius_in_meters = radius * conv[unit]
        
        entry = data_store.get(key)
        if not entry or not isinstance(entry[0], RedisSortedSet):
            return b"*0\r\n"
        
        zset = entry[0]
        results = []
        for member, score in zset.members.items():
            lon, lat = geohash_decode(int(score))
            dist = haversine_distance(center_lon, center_lat, lon, lat)
            if dist <= radius_in_meters:
                results.append(member)
        
        return encode_resp_array(results)
    elif cmd == b"INFO":
        if args and args[0].upper() == b"REPLICATION":
            res_parts = [
                f"role:{config['role']}",
                f"master_replid:{config['master_replid']}",
                f"master_repl_offset:{config['master_repl_offset']}"
            ]
            res_str = "\n".join(res_parts)
            return b"$" + str(len(res_str)).encode() + b"\r\n" + res_str.encode() + b"\r\n"
        return b"-ERR syntax error\r\n"
    elif cmd == b"ECHO":
        if not args: return b"-ERR wrong number of arguments for 'echo' command\r\n"
        msg = args[0]
        return b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
    elif cmd == b"SET":
        if len(args) < 2: return b"-ERR wrong number of arguments for 'set' command\r\n"
        key, val = args[0], args[1]
        expiry = None
        if len(args) >= 4 and args[2].upper() == b"PX":
            try: expiry = time.time() + (int(args[3])/1000)
            except ValueError: pass
        with data_condition:
            data_store[key] = (val, expiry)
            mark_modified(key)
        return b"+OK\r\n"
    elif cmd == b"GET":
        if not args: return b"-ERR wrong number of arguments for 'get' command\r\n"
        key = args[0]
        entry = data_store.get(key)
        if entry:
            val, exp = entry
            if exp and time.time() > exp:
                with data_condition:
                    del data_store[key]
                    mark_modified(key)
                return b"$-1\r\n"
            else:
                if isinstance(val, (list, RedisStream, RedisSortedSet)): return b"$-1\r\n"
                else: return b"$" + str(len(val)).encode() + b"\r\n" + val + b"\r\n"
        else: return b"$-1\r\n"
    elif cmd == b"INCR":
        if not args: return b"-ERR wrong number of arguments for 'incr' command\r\n"
        key = args[0]
        with data_condition:
            entry = data_store.get(key)
            if entry:
                val, exp = entry
                try:
                    new_val = int(val) + 1
                    data_store[key] = (str(new_val).encode(), exp)
                    mark_modified(key)
                    return f":{new_val}\r\n".encode()
                except (ValueError, TypeError):
                    return b"-ERR value is not an integer or out of range\r\n"
            else:
                data_store[key] = (b"1", None)
                mark_modified(key)
                return b":1\r\n"
    elif cmd == b"TYPE":
        if not args: return b"+none\r\n"
        key = args[0]
        entry = data_store.get(key)
        if not entry: return b"+none\r\n"
        else:
            v = entry[0]
            if isinstance(v, list): return b"+list\r\n"
            elif isinstance(v, RedisStream): return b"+stream\r\n"
            elif isinstance(v, RedisSortedSet): return b"+zset\r\n"
            else: return b"+string\r\n"
    elif cmd == b"XADD":
        if len(args) < 4: return b"-ERR wrong number of arguments for 'xadd' command\r\n"
        key, id_str = args[0], args[1]
        f_dict = {}
        for i in range(2, len(args), 2):
            if i+1 < len(args): f_dict[args[i]] = args[i+1]
        with data_condition:
            if key not in data_store: data_store[key] = (RedisStream(), None)
            stream = data_store[key][0]
            if not isinstance(stream, RedisStream): return b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            success, res = stream.add_entry(id_str, f_dict)
            if success:
                mark_modified(key)
                data_condition.notify_all()
                return b"$" + str(len(res)).encode() + b"\r\n" + res + b"\r\n"
            else: return b"-" + res.encode() + b"\r\n"
    elif cmd == b"XRANGE":
        if len(args) < 3: return b"-ERR wrong number of arguments for 'xrange' command\r\n"
        key, start, end = args[0], args[1], args[2]
        entry = data_store.get(key)
        if not entry or not isinstance(entry[0], RedisStream): return b"*0\r\n"
        else:
            stream = entry[0]
            results = stream.get_range(start, end)
            res = f"*{len(results)}\r\n".encode()
            for eid, fields in results: res += encode_stream_entry(eid, fields)
            return res
    elif cmd == b"XREAD":
        block_ms = None
        streams_idx = -1
        for i, arg in enumerate(args):
            if arg.upper() == b"BLOCK" and i+1 < len(args): block_ms = int(args[i+1])
            if arg.upper() == b"STREAMS": streams_idx = i; break
        if streams_idx == -1: return b"-ERR missing STREAMS keyword\r\n"
        remaining = args[streams_idx+1:]
        num_keys = len(remaining) // 2
        actual_keys = remaining[:num_keys]
        raw_ids = remaining[num_keys:]
        actual_ids = []
        for k, tid in zip(actual_keys, raw_ids):
            if tid == b"$":
                entry = data_store.get(k)
                if entry and isinstance(entry[0], RedisStream):
                    lt, ls = entry[0].last_id
                    actual_ids.append(f"{lt}-{ls}".encode())
                else: actual_ids.append(b"0-0")
            else: actual_ids.append(tid)

        def get_results():
            results = []
            for k, tid in zip(actual_keys, actual_ids):
                entry = data_store.get(k)
                if entry and isinstance(entry[0], RedisStream):
                    items = entry[0].get_after(tid)
                    if items: results.append((k, items))
            return results

        with data_condition:
            final_results = get_results()
            if not final_results and block_ms is not None:
                if block_ms == 0:
                    while not final_results:
                        data_condition.wait()
                        final_results = get_results()
                else:
                    data_condition.wait(timeout=block_ms/1000)
                    final_results = get_results()
        if not final_results: return b"*-1\r\n"
        else:
            res = f"*{len(final_results)}\r\n".encode()
            for k, items in final_results:
                res += b"*2\r\n" + b"$" + str(len(k)).encode() + b"\r\n" + k + b"\r\n"
                res += f"*{len(items)}\r\n".encode()
                for eid, fields in items: res += encode_stream_entry(eid, fields)
            return res
    elif cmd == b"RPUSH" or cmd == b"LPUSH":
        if len(args) < 2: return b"-ERR wrong number of arguments\r\n"
        key, new_vals = args[0], args[1:]
        with data_condition:
            if key not in data_store: data_store[key] = ([], None)
            d_list, _ = data_store[key]
            if not isinstance(d_list, list): return b"-ERR WRONGTYPE\r\n"
            for v in new_vals:
                if cmd == b"RPUSH": d_list.append(v)
                else: d_list.insert(0, v)
            mark_modified(key)
            data_condition.notify_all()
        return f":{len(d_list)}\r\n".encode()
    elif cmd == b"LRANGE":
        if len(args) < 3: return b"-ERR wrong number of arguments\r\n"
        key, s, e = args[0], int(args[1]), int(args[2])
        entry = data_store.get(key)
        if not entry or not isinstance(entry[0], list): return b"*0\r\n"
        else:
            l = entry[0]
            def norm(i, n):
                if i < 0: i = n + i
                return max(0, min(i, n-1))
            if not l: return b"*0\r\n"
            si, ei = norm(s, len(l)), norm(e, len(l))
            sub = [] if (s >= len(l) or (s >= 0 and s > e and e >= 0) or (s < 0 and e < 0 and s > e)) else l[si : ei + 1]
            if s >= 0 and e >= 0 and s > e: sub = []
            res = f"*{len(sub)}\r\n".encode()
            for i in sub: res += b"$" + str(len(i)).encode() + b"\r\n" + i + b"\r\n"
            return res
    elif cmd == b"LLEN":
        if not args: return b":0\r\n"
        entry = data_store.get(args[0])
        return f":{len(entry[0])}\r\n".encode() if entry and isinstance(entry[0], list) else b":0\r\n"
    elif cmd == b"LPOP":
        if not args: return b"$-1\r\n"
        key, cnt = args[0], int(args[1]) if len(args) > 1 else None
        with data_condition:
            entry = data_store.get(key)
            if not entry or not isinstance(entry[0], list) or len(entry[0]) == 0: return b"$-1\r\n"
            else:
                l = entry[0]
                if cnt is None:
                    v = l.pop(0)
                    mark_modified(key)
                    return b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
                else:
                    p = [l.pop(0) for _ in range(min(cnt, len(l)))]
                    if p: mark_modified(key)
                    res = f"*{len(p)}\r\n".encode()
                    for i in p: res += b"$" + str(len(i)).encode() + b"\r\n" + i + b"\r\n"
                    return res
    elif cmd == b"BLPOP":
        if len(args) < 2: return b"*-1\r\n"
        key, tval = args[0], float(args[-1])
        def get_p():
            ent = data_store.get(key)
            return ent[0].pop(0) if ent and isinstance(ent[0], list) and len(ent[0]) > 0 else None
        with data_condition:
            v = get_p()
            if tval == 0:
                while v is None: data_condition.wait(); v = get_p()
            else:
                if v is None: data_condition.wait(timeout=tval); v = get_p()
            if v is not None:
                mark_modified(key)
        if v is None: return b"*-1\r\n"
        else: return f"*2\r\n${len(key)}\r\n".encode() + key + b"\r\n" + b"$" + str(len(v)).encode() + b"\r\n" + v + b"\r\n"
    return b"-ERR unknown command\r\n"

def parse_resp(data):
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
    except:
        return None, b"", 0

def handle_client(client_connection):
    in_transaction = False
    transaction_queue = []
    watched_keys = {}
    subscribed_channels = set()

    try:
        while True:
            data = client_connection.recv(4096)
            if not data: break
            
            while data:
                args, rest, _ = parse_resp(data)
                if not args: break
                data = rest
                
                cmd = args[0].upper()
                cmd_args = args[1:]
                
                allowed_in_subscribed = {b"SUBSCRIBE", b"UNSUBSCRIBE", b"PSUBSCRIBE", b"PUNSUBSCRIBE", b"PING", b"QUIT", b"RESET"}
                if subscribed_channels and cmd not in allowed_in_subscribed:
                    client_connection.send(f"-ERR Can't execute '{cmd.decode().lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n".encode())
                    continue

                if cmd == b"MULTI":
                    in_transaction = True
                    client_connection.send(b"+OK\r\n")
                elif cmd == b"DISCARD":
                    if not in_transaction:
                        client_connection.send(b"-ERR DISCARD without MULTI\r\n")
                    else:
                        in_transaction = False
                        transaction_queue = []
                        watched_keys = {}
                        client_connection.send(b"+OK\r\n")
                elif cmd == b"EXEC":
                    if not in_transaction:
                        client_connection.send(b"-ERR EXEC without MULTI\r\n")
                    else:
                        abort_transaction = False
                        for k, v in watched_keys.items():
                            if key_versions.get(k, 0) != v:
                                abort_transaction = True
                                break
                        
                        if abort_transaction:
                            client_connection.send(b"*-1\r\n")
                        else:
                            if not transaction_queue:
                                client_connection.send(b"*0\r\n")
                            else:
                                res = f"*{len(transaction_queue)}\r\n".encode()
                                for q_cmd, q_args in transaction_queue:
                                    res += process_command(q_cmd, q_args)
                                client_connection.send(res)
                                for q_cmd, q_args in transaction_queue:
                                    if is_write_command(q_cmd):
                                        propagate(q_cmd, q_args)
                                        write_to_aof([q_cmd] + list(q_args))
                        
                        in_transaction = False
                        transaction_queue = []
                        watched_keys = {}
                elif cmd == b"WATCH":
                    if in_transaction:
                        client_connection.send(b"-ERR WATCH inside MULTI is not allowed\r\n")
                    else:
                        for k in cmd_args:
                            if k not in watched_keys:
                                watched_keys[k] = key_versions.get(k, 0)
                        client_connection.send(b"+OK\r\n")
                elif cmd == b"UNWATCH":
                    watched_keys = {}
                    client_connection.send(b"+OK\r\n")
                elif cmd == b"PING" and subscribed_channels:
                    client_connection.send(encode_resp_array([b"pong", b""]))
                elif cmd == b"SUBSCRIBE":
                    with pubsub_lock:
                        for channel in cmd_args:
                            subscribed_channels.add(channel)
                            if channel not in pubsub_subscriptions:
                                pubsub_subscriptions[channel] = set()
                            pubsub_subscriptions[channel].add(client_connection)
                            res = encode_resp_array([b"subscribe", channel, len(subscribed_channels)])
                            client_connection.send(res)
                elif cmd == b"UNSUBSCRIBE":
                    channels_to_unsub = cmd_args if cmd_args else list(subscribed_channels)
                    with pubsub_lock:
                        for channel in channels_to_unsub:
                            if channel in subscribed_channels:
                                subscribed_channels.remove(channel)
                                if channel in pubsub_subscriptions:
                                    pubsub_subscriptions[channel].discard(client_connection)
                                    if not pubsub_subscriptions[channel]:
                                        del pubsub_subscriptions[channel]
                            res = encode_resp_array([b"unsubscribe", channel, len(subscribed_channels)])
                            client_connection.send(res)
                elif cmd == b"PSYNC":
                    resp = process_command(cmd, cmd_args)
                    client_connection.send(resp)
                    replica_info = {"conn": client_connection, "ack_offset": 0}
                    with replicas_lock:
                        replicas.append(replica_info)
                    while True:
                        rep_data = client_connection.recv(4096)
                        if not rep_data: break
                        while rep_data:
                            r_args, r_rest, _ = parse_resp(rep_data)
                            if not r_args: break
                            rep_data = r_rest
                            if r_args[0].upper() == b"REPLCONF" and len(r_args) >= 3 and r_args[1].upper() == b"ACK":
                                with replicas_lock:
                                    replica_info["ack_offset"] = int(r_args[2])
                                    replicas_condition.notify_all()
                    return 
                elif cmd == b"WAIT":
                    num_replicas_needed = int(cmd_args[0])
                    timeout_ms = int(cmd_args[1])
                    current_offset = config["master_repl_offset"]
                    def get_in_sync_count():
                        count = 0
                        for r in replicas:
                            if r["ack_offset"] >= current_offset:
                                count += 1
                        return count
                    start_time = time.time()
                    with replicas_lock:
                        if current_offset == 0:
                            client_connection.send(f":{len(replicas)}\r\n".encode())
                        else:
                            getack_cmd = encode_resp_array([b"REPLCONF", b"GETACK", b"*"])
                            for r in replicas:
                                r["conn"].send(getack_cmd)
                            while True:
                                count = get_in_sync_count()
                                if count >= num_replicas_needed:
                                    break
                                elapsed = (time.time() - start_time) * 1000
                                if elapsed >= timeout_ms:
                                    break
                                replicas_condition.wait(timeout=(timeout_ms - elapsed) / 1000)
                            final_count = get_in_sync_count()
                            client_connection.send(f":{final_count}\r\n".encode())
                elif cmd == b"PUBLISH":
                    if len(cmd_args) < 2:
                        client_connection.send(b"-ERR wrong number of arguments for 'publish' command\r\n")
                    else:
                        channel = cmd_args[0]
                        message = cmd_args[1]
                        with pubsub_lock:
                            subs = pubsub_subscriptions.get(channel, set()).copy()
                            count = len(subs)
                            client_connection.send(f":{count}\r\n".encode())
                            msg_bytes = encode_resp_array([b"message", channel, message])
                            for sub_conn in subs:
                                try:
                                    sub_conn.send(msg_bytes)
                                except:
                                    pass
                else:
                    if in_transaction:
                        transaction_queue.append((cmd, cmd_args))
                        client_connection.send(b"+QUEUED\r\n")
                    else:
                        resp = process_command(cmd, cmd_args)
                        client_connection.send(resp)
                        if is_write_command(cmd):
                            propagate(cmd, cmd_args)
                            write_to_aof([cmd] + list(cmd_args))
    except (ConnectionResetError, IndexError, ValueError, socket.error): pass
    finally:
        with pubsub_lock:
            for channel in subscribed_channels:
                if channel in pubsub_subscriptions:
                    pubsub_subscriptions[channel].discard(client_connection)
                    if not pubsub_subscriptions[channel]:
                        del pubsub_subscriptions[channel]
        with replicas_lock:
            for r in replicas:
                if r["conn"] == client_connection:
                    replicas.remove(r)
                    break
        client_connection.close()

def replica_manager():
    host = config["master_host"]
    port = config["master_port"]
    replica_port = str(config["port"]).encode()
    offset = 0
    with socket.create_connection((host, port)) as master_conn:
        master_conn.send(b"*1\r\n$4\r\nPING\r\n")
        master_conn.recv(4096)
        master_conn.send(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + str(len(replica_port)).encode() + b"\r\n" + replica_port + b"\r\n")
        master_conn.recv(4096)
        master_conn.send(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        master_conn.recv(4096)
        master_conn.send(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        resp = master_conn.recv(4096)
        
        def consume_rdb(data, conn):
            if b"$" not in data:
                data = conn.recv(4096)
            idx = data.find(b"$")
            header = data[idx+1:]
            end_header = header.find(b"\r\n")
            while end_header == -1:
                header += conn.recv(4096)
                end_header = header.find(b"\r\n")
            rdb_len = int(header[:end_header])
            content = header[end_header+2:]
            while len(content) < rdb_len:
                content += conn.recv(4096)
            return content[rdb_len:]

        remaining_data = consume_rdb(resp, master_conn)
        
        while True:
            if not remaining_data:
                remaining_data = master_conn.recv(4096)
            if not remaining_data: break
            
            while remaining_data:
                args, rest, consumed = parse_resp(remaining_data)
                if not args: break
                remaining_data = rest
                
                cmd = args[0].upper()
                cmd_args = args[1:]
                
                if cmd == b"REPLCONF" and cmd_args and cmd_args[0].upper() == b"GETACK":
                    master_conn.send(encode_resp_array([b"REPLCONF", b"ACK", str(offset).encode()]))
                
                process_command(cmd, cmd_args)
                offset += consumed

def get_active_aof_file():
    aof_dir = os.path.join(config["dir"], config["appenddirname"])
    manifest_path = os.path.join(aof_dir, config["appendfilename"] + ".manifest")
    if not os.path.exists(manifest_path):
        return None
    with open(manifest_path, "r") as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 6 and parts[0] == "file" and parts[1].endswith(".aof"):
                return os.path.join(aof_dir, parts[1])
    return None

def load_aof():
    if config["appendonly"] != "yes":
        return
    active_aof = get_active_aof_file()
    if not active_aof or not os.path.exists(active_aof):
        return
    
    with open(active_aof, "rb") as f:
        data = f.read()
        while data:
            args, rest, consumed = parse_resp(data)
            if not args:
                break
            cmd = args[0].upper()
            cmd_args = args[1:]
            process_command(cmd, cmd_args)
            data = rest

def main():
    global aof_handle
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str)
    parser.add_argument("--dir", type=str)
    parser.add_argument("--dbfilename", type=str, default="dump.rdb")
    parser.add_argument("--appendonly", type=str, default="no")
    parser.add_argument("--appenddirname", type=str, default="appendonlydir")
    parser.add_argument("--appendfilename", type=str, default="appendonly.aof")
    parser.add_argument("--appendfsync", type=str, default="everysec")
    args = parser.parse_args()
    config["port"] = args.port
    config["dir"] = args.dir if args.dir else os.getcwd()
    config["dbfilename"] = args.dbfilename
    config["appendonly"] = args.appendonly
    config["appenddirname"] = args.appenddirname
    config["appendfilename"] = args.appendfilename
    config["appendfsync"] = args.appendfsync
    
    if config["appendonly"] == "yes":
        aof_dir = os.path.join(config["dir"], config["appenddirname"])
        os.makedirs(aof_dir, exist_ok=True)
        manifest_file = os.path.join(aof_dir, config["appendfilename"] + ".manifest")
        if not os.path.exists(manifest_file):
            aof_file = os.path.join(aof_dir, config["appendfilename"] + ".1.incr.aof")
            if not os.path.exists(aof_file):
                with open(aof_file, "w"): pass
            with open(manifest_file, "w") as f:
                f.write(f"file {config['appendfilename']}.1.incr.aof seq 1 type i\n")
    
    load_aof()
    load_rdb()
    
    if config["appendonly"] == "yes":
        active_aof = get_active_aof_file()
        if active_aof:
            aof_handle = open(active_aof, "ab")
    
    if args.replicaof:
        config["role"] = "slave"
        m_host, m_port = args.replicaof.split()
        config["master_host"] = m_host
        config["master_port"] = int(m_port)
        threading.Thread(target=replica_manager).start()
    server_socket = socket.create_server(("localhost", args.port), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,)).start()

if __name__ == "__main__": main()
