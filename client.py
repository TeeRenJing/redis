#!/usr/bin/env python3
import socket
import time
import argparse

# -----------------------
# Minimal RESP utilities
# -----------------------
def encode_resp(*args: str) -> bytes:
    """Encode a command in RESP array format."""
    out = f"*{len(args)}\r\n"
    for a in args:
        a = str(a)
        out += f"${len(a)}\r\n{a}\r\n"
    return out.encode()

def _readline(sock: socket.socket) -> bytes:
    """Read until CRLF."""
    buf = bytearray()
    while True:
        ch = sock.recv(1)
        if not ch:
            raise ConnectionError("Connection closed while reading line")
        buf += ch
        if len(buf) >= 2 and buf[-2:] == b"\r\n":
            return bytes(buf[:-2])

def _readn(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed while reading bytes")
        buf += chunk
    return bytes(buf)

def decode_resp(sock: socket.socket):
    """
    Very small RESP reader that can handle:
      + Simple String
      - Error
      : Integer
      $ Bulk / Nil
      * Array  (recursively)
    Returns Python values:
      + -> ('simple', str)
      - -> ('error', str)
      : -> ('int', int)
      $ -> ('bulk', bytes) or ('nil', None)
      * -> ('array', list_of_values)
    """
    first = sock.recv(1)
    if not first:
        raise ConnectionError("Connection closed before type byte")
    t = first

    if t == b'+':
        line = _readline(sock).decode('utf-8', 'replace')
        return ('simple', line)
    elif t == b'-':
        line = _readline(sock).decode('utf-8', 'replace')
        return ('error', line)
    elif t == b':':
        line = _readline(sock).decode()
        return ('int', int(line))
    elif t == b'$':
        line = _readline(sock).decode()
        length = int(line)
        if length == -1:
            return ('nil', None)
        data = _readn(sock, length)
        crlf = _readn(sock, 2)  # consume trailing CRLF
        return ('bulk', data)
    elif t == b'*':
        line = _readline(sock).decode()
        n = int(line)
        arr = []
        for _ in range(n):
            arr.append(decode_resp(sock))
        return ('array', arr)
    else:
        # Fallback: read rest of line for debugging
        rest = _readline(sock)
        raise ValueError(f"Unknown RESP type byte: {t!r}, rest={rest!r}")

# -----------------------
# Test driver
# -----------------------
def roundtrip(sock, *cmd):
    """Send a command and decode one reply."""
    sock.sendall(encode_resp(*cmd))
    return decode_resp(sock)

def main():
    ap = argparse.ArgumentParser(description="Expiry test client for custom Redis-like server")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--key", default="pineapple")
    ap.add_argument("--value", default="apple")
    ap.add_argument("--px", type=int, default=100, help="Expiry in ms")
    ap.add_argument("--sleep-ms", type=int, default=101, help="Sleep before second GET")
    args = ap.parse_args()

    with socket.create_connection((args.host, args.port), timeout=5) as sock:
        # 1) SET key value PX <ms>
        print(f"> SET {args.key} {args.value} PX {args.px}")
        r1 = roundtrip(sock, "SET", args.key, args.value, "PX", str(args.px))
        print("<", r1)

        # 2) Immediate GET (should NOT be expired)
        print(f"> GET {args.key}")
        r2 = roundtrip(sock, "GET", args.key)
        print("<", r2)
        if r2[0] == 'bulk':
            print(f"OK: got bulk '{r2[1].decode()}' immediately")
        else:
            print("WARN: expected bulk right after SET")

        # 3) Sleep to cross expiry boundary
        sleep_seconds = args.sleep_ms / 1000.0
        print(f"Sleeping for {args.sleep_ms}ms...")
        time.sleep(sleep_seconds)

        # 4) GET after expiry (should be NIL)
        print(f"> GET {args.key}")
        r3 = roundtrip(sock, "GET", args.key)
        print("<", r3)
        if r3[0] == 'nil':
            print("PASS: got NIL after expiry")
        else:
            print("FAIL: expected NIL after expiry")

if __name__ == "__main__":
    main()
