#!/usr/bin/env python3
"""Local-only CLI SIGINT test. No third-party Python dependencies or external network."""
import argparse
import base64
import hashlib
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import socketserver
import struct
import subprocess
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "target/debug/orderbook-collector"
STOP = threading.Event()
READY = threading.Event()
CLOSED = threading.Event()
CONNECTIONS = set()
LOCK = threading.Lock()


def send_frame(sock, opcode, payload=b""):
    if isinstance(payload, str):
        payload = payload.encode()
    size = len(payload)
    header = bytes([0x80 | opcode])
    if size < 126:
        header += bytes([size])
    elif size < 65536:
        header += bytes([126]) + struct.pack("!H", size)
    else:
        header += bytes([127]) + struct.pack("!Q", size)
    sock.sendall(header + payload)


def exact(sock, size):
    data = bytearray()
    while len(data) < size:
        part = sock.recv(size - len(data))
        if not part:
            raise EOFError
        data.extend(part)
    return bytes(data)


def receive(sock):
    head = exact(sock, 2)
    opcode, size = head[0] & 15, head[1] & 127
    if size == 126:
        size = struct.unpack("!H", exact(sock, 2))[0]
    elif size == 127:
        size = struct.unpack("!Q", exact(sock, 8))[0]
    if size > 8 << 20:
        raise ValueError("oversized frame")
    mask = exact(sock, 4) if head[1] & 128 else None
    data = exact(sock, size)
    if mask:
        data = bytes(c ^ mask[n % 4] for n, c in enumerate(data))
    return opcode, data


class Gamma(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"markets": [{"id": "local", "version": "v1", "active": True,
            "closed": False, "enableOrderBook": True, "acceptingOrders": True,
            "clobTokenIds": '["a","b"]'}], "next_cursor": None}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


class Market(socketserver.BaseRequestHandler):
    def handle(self):
        sock = self.request
        sock.settimeout(2)
        with LOCK:
            CONNECTIONS.add(sock)
        try:
            header = bytearray()
            while b"\r\n\r\n" not in header:
                header.extend(exact(sock, 1))
                if len(header) > 65536:
                    raise ValueError("oversized handshake")
            headers = dict(line.split(":", 1) for line in header.decode().split("\r\n")[1:] if ":" in line)
            key = next(value.strip() for name, value in headers.items() if name.lower() == "sec-websocket-key")
            accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
            sock.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n").encode())
            while not STOP.is_set():
                try:
                    opcode, data = receive(sock)
                except socket.timeout:
                    continue
                if opcode == 8:
                    send_frame(sock, 8, data)
                    CLOSED.set()
                    break
                if opcode == 9:
                    send_frame(sock, 10, data)
                elif opcode == 1 and data == b"PING":
                    send_frame(sock, 1, "PONG")
                elif opcode == 1:
                    subscription = json.loads(data)
                    if subscription.get("operation") == "unsubscribe":
                        continue
                    ids = subscription.get("assets_ids", [])
                    now = str(int(time.time() * 1000))
                    send_frame(sock, 1, json.dumps([{"event_type": "book", "asset_id": asset,
                        "timestamp": now, "bids": [{"price": "0.4", "size": "5"}],
                        "asks": [{"price": "0.6", "size": "8"}]} for asset in ids]))
                    READY.set()
        except (EOFError, ConnectionError, OSError):
            pass
        finally:
            with LOCK:
                CONNECTIONS.discard(sock)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-minute", action="store_true", help="also observe the real 60s live snapshot timer")
    args = parser.parse_args()
    gamma = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Gamma)
    market = Server(("127.0.0.1", 0), Market)
    servers = [gamma, market]
    threads = [threading.Thread(target=s.serve_forever, name="orderbook-local-fixture") for s in servers]
    ports = [s.server_address[1] for s in servers]
    process = None
    fixture_started = subprocess.check_output(["ps", "-p", str(os.getpid()), "-o", "lstart="], text=True).strip()
    start = time.monotonic()
    try:
        for thread in threads:
            thread.start()
        with tempfile.TemporaryDirectory(prefix="orderbook-sigint-") as tmp, tempfile.TemporaryFile() as logs:
            output = str(Path(tmp) / "result.sbe")
            process = subprocess.Popen([str(BIN), "--no-stdout", "--output", output, "live",
                "--discovery-url", f"http://127.0.0.1:{ports[0]}/markets/keyset",
                "--websocket-url", f"ws://127.0.0.1:{ports[1]}/ws/market",
                "--replicas", "1", "--max-symbols", "2"], stdout=subprocess.PIPE, stderr=logs)
            pid = process.pid
            print(json.dumps({"fixture_owner":"orderbook-shutdown-test", "fixture_pid":os.getpid(), "fixture_start_time":fixture_started,
                "ports":ports, "collector_pid":pid, "cleanup":"finally: SIGINT collector, reap Popen, shutdown+close servers, join threads"}), flush=True)
            started = subprocess.check_output(["ps", "-p", str(pid), "-o", "lstart="], text=True).strip()
            if not READY.wait(10):
                logs.seek(0)
                raise AssertionError("no local snapshot within 10s: " + logs.read(8000).decode(errors="replace"))
            if args.verify_minute:
                deadline = time.monotonic() + 61
                while time.monotonic() < deadline:
                    assert process.poll() is None, "collector exited before minute snapshot"
                    time.sleep(min(1, max(0, deadline - time.monotonic())))
            else:
                time.sleep(0.2)
            process.send_signal(signal.SIGINT)
            stdout, _ = process.communicate(timeout=10)
            logs.seek(0)
            stderr = logs.read(16000).decode(errors="replace")
            assert process.returncode == 0, stderr
            assert stdout == b"", "file-only mode contaminated stdout"
            assert CLOSED.wait(2), "client did not send WebSocket Close"
            inspected = subprocess.run([str(BIN), "inspect", "--input", output], capture_output=True, check=True, timeout=5)
            rows = [json.loads(line) for line in inspected.stdout.splitlines()]
            assert {row["symbol"] for row in rows} == {"a", "b"}
            if args.verify_minute:
                snapshots = [row for row in rows if row["type"] == "snapshot"]
                assert len(snapshots) >= 4, "missing real timer snapshots"
                from datetime import datetime
                for symbol in ("a", "b"):
                    times = [datetime.fromisoformat(row["received_at"]) for row in snapshots if row["symbol"] == symbol]
                    assert (max(times) - min(times)).total_seconds() >= 59, "snapshot did not span a minute"
                minutes = [json.loads(line) for line in stderr.splitlines() if line.startswith('{')]
                minutes = [entry for entry in minutes if entry.get("mode") == "minute_snapshot"]
                assert minutes, "minute diagnostic missing"
                for entry in minutes:
                    batch = rows[entry["record_start"]:entry["record_end"]]
                    assert len(batch) == entry["symbols"] == 2
                    assert [row["symbol"] for row in batch] == ["a", "b"]
                    assert all(row["type"] == "snapshot" for row in batch)
            assert "live_shutdown" in stderr
            print(json.dumps({"test": "local_cli_sigint", "result": "passed", "pid": pid,
                "start_time": started, "ports": ports, "stop_command": f"kill -INT {pid} (already exited/reaped)",
                "websocket_close_observed": True, "real_minute_timer_verified": args.verify_minute, "decoded_records": len(rows),
                "elapsed_seconds": round(time.monotonic() - start, 3)}))
    finally:
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        STOP.set()
        with LOCK:
            for sock in list(CONNECTIONS):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        for server in servers:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=3)
            assert not thread.is_alive(), "fixture thread leaked"
        for port in ports:
            with socket.socket() as probe:
                probe.settimeout(0.5)
                assert probe.connect_ex(("127.0.0.1", port)) != 0, "fixture port still listening"
        print(json.dumps({"cleanup": "verified", "ports_closed": ports, "child_reaped": process is None or process.poll() is not None}))


if __name__ == "__main__":
    main()
