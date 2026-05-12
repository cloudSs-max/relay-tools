#!/usr/bin/env python3
"""
github_tunnel.py  —  SSH tunnel through GitHub API
====================================================
Both sides only need HTTPS access to api.github.com (port 443).
No Microsoft, no Cloudflare, no blocked infrastructure.

USAGE
-----
Server (Iranian server, run via noVNC console):
    python3 github_tunnel.py server --token TOKEN

Client (your machine outside Iran):
    python3 github_tunnel.py client --token TOKEN
    Then: ssh -p 2222 root@localhost

HOW IT WORKS
------------
  GitHub repo acts as a message relay:
    tunnel/sess/c2s.bin  ← client writes, server reads
    tunnel/sess/s2c.bin  ← server writes, client reads

  ETag-based conditional GET:  if no new data → 304 response → FREE (no rate limit)
  Actual data writes:          ~2 API credits per SSH round-trip (well within 5000/hr)
  Latency:                     ~300-700ms  (usable for interactive SSH)
"""

import sys, os, time, json, socket, threading, base64, zlib, argparse, queue, select, struct
import urllib.request, urllib.error, ssl

# ── Configuration ─────────────────────────────────────────────────────────────
REPO          = "cloudSs-max/relay-tools"
SESSION       = "sess1"
POLL_SEC      = 0.15       # seconds between polls  (150ms)
LOCAL_SSH     = 22         # sshd port on Iranian server
CLIENT_PORT   = 2222       # local port exposed on Sohrab's machine
API_HOST      = "api.github.com"
# ──────────────────────────────────────────────────────────────────────────────

CTX = ssl.create_default_context()


def _api(method, path, token, body=None, etag=None):
    """Raw GitHub API call — uses only Python stdlib (no pip needed)."""
    url = f"https://{API_HOST}/repos/{path}"
    req = urllib.request.Request(url, method=method)
    req.add_header("Authorization",  f"token {token}")
    req.add_header("Accept",         "application/vnd.github.v3+json")
    req.add_header("User-Agent",     "gh-tunnel/2.0")
    if etag:
        req.add_header("If-None-Match", etag)
    if body:
        data = json.dumps(body).encode()
        req.add_header("Content-Type",   "application/json")
        req.add_header("Content-Length", str(len(data)))
        req.data = data
    try:
        with urllib.request.urlopen(req, context=CTX, timeout=20) as r:
            raw  = r.read()
            etag = r.headers.get("ETag", "")
            return r.status, (json.loads(raw) if raw else {}), etag
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, (json.loads(raw) if raw else {}), ""
    except Exception as e:
        return 0, {}, ""


class Channel:
    """
    One-directional data channel backed by a single GitHub file.
    Only ONE side writes; both sides can read.
    Thread-safe writes via lock.
    """

    def __init__(self, repo, token, path, direction):
        self.repo  = repo
        self.token = token
        self.path  = path
        self.dir   = direction      # "write" or "read" (for logging)
        self._sha  = None           # SHA needed for in-place update
        self._etag = None           # for conditional GET
        self._last  = 0             # last sequence number consumed
        self._seq   = 0             # our write counter
        self._lock  = threading.Lock()

    # ── Read ──────────────────────────────────────────────────────────────────

    def read_new(self):
        """Returns bytes if new data available, None otherwise. 304 = free."""
        code, body, etag = _api(
            "GET",
            f"{self.repo}/contents/{self.path}",
            self.token,
            etag=self._etag
        )
        if code == 304:
            return None                              # not modified → free poll
        if code == 200:
            self._etag = etag
            self._sha  = body.get("sha")
            raw = base64.b64decode(body["content"].replace("\n", ""))
            if len(raw) < 8:
                return None
            seq   = int.from_bytes(raw[:4], "big")
            flags = int.from_bytes(raw[4:8], "big")
            pay   = raw[8:]
            if flags & 1:
                pay = zlib.decompress(pay)
            if seq > self._last:
                self._last = seq
                return pay
        return None

    # ── Write ─────────────────────────────────────────────────────────────────

    def write(self, data: bytes, retries=5) -> bool:
        """Write data to the channel. Returns True on success."""
        with self._lock:
            self._seq += 1
            seq = self._seq
            sha = self._sha

        comp = zlib.compress(data, 1)
        pay, flags = (comp, 1) if len(comp) < len(data) else (data, 0)
        pkt = seq.to_bytes(4, "big") + flags.to_bytes(4, "big") + pay

        body = {
            "message": "t",
            "content": base64.b64encode(pkt).decode(),
            "branch":  "main"
        }
        if sha:
            body["sha"] = sha

        for attempt in range(retries):
            code, resp, _ = _api(
                "PUT",
                f"{self.repo}/contents/{self.path}",
                self.token,
                body=body
            )
            if code in (200, 201):
                with self._lock:
                    self._sha = resp.get("content", {}).get("sha")
                return True
            if code in (409, 422):       # conflict or missing sha → re-fetch and retry
                self._refresh_sha()
                body["sha"] = self._sha
            time.sleep(0.3 * (attempt + 1))
        return False

    def _refresh_sha(self):
        code, body, etag = _api(
            "GET",
            f"{self.repo}/contents/{self.path}",
            self.token
        )
        if code == 200:
            self._sha  = body.get("sha")
            self._etag = etag


# ── Pump ──────────────────────────────────────────────────────────────────────

def pump(sock, recv_ch: Channel, send_ch: Channel, tag=""):
    """
    Bidirectional pump between TCP socket and two GitHub channels.
      recv_ch  →  we read from GitHub, write to socket
      send_ch  ←  we read from socket, write to GitHub
    """
    done = threading.Event()
    sq   = queue.Queue(maxsize=128)

    def _sock_reader():
        buf = b""
        sock.setblocking(False)
        while not done.is_set():
            try:
                chunk = sock.recv(8192)
                if not chunk:
                    break
                buf += chunk
            except BlockingIOError:
                if buf:
                    sq.put(buf)
                    buf = b""
                time.sleep(0.02)
            except Exception:
                break
        if buf:
            sq.put(buf)
        sq.put(None)

    def _gh_sender():
        while not done.is_set():
            try:
                data = sq.get(timeout=0.3)
                if data is None:
                    break
                ok = send_ch.write(data)
                if not ok:
                    print(f"[{tag}] write failed — retrying later")
            except queue.Empty:
                pass

    def _gh_receiver():
        while not done.is_set():
            data = recv_ch.read_new()
            if data:
                if data == b"\xff\xfe\xfd\xfc":    # EOF signal
                    break
                try:
                    sock.sendall(data)
                except Exception:
                    break
            else:
                time.sleep(POLL_SEC)

    t1 = threading.Thread(target=_sock_reader,  daemon=True)
    t2 = threading.Thread(target=_gh_sender,    daemon=True)
    t3 = threading.Thread(target=_gh_receiver,  daemon=True)

    for t in (t1, t2, t3):
        t.start()

    t1.join()          # wait until socket closes
    done.set()
    for t in (t2, t3):
        t.join(timeout=5)

    sock.close()
    print(f"[{tag}] connection closed")


# ── Server mode ───────────────────────────────────────────────────────────────

def run_server(token):
    print("=" * 55)
    print("  GitHub API SSH Tunnel  —  SERVER MODE")
    print("=" * 55)
    print(f"  Repo   : {REPO}")
    print(f"  Session: {SESSION}")
    print(f"  SSH    : 127.0.0.1:{LOCAL_SSH}")
    print()

    c2s = Channel(REPO, token, f"tunnel/{SESSION}/c2s.bin", "read")
    s2c = Channel(REPO, token, f"tunnel/{SESSION}/s2c.bin", "write")

    # Signal ready to client
    print("[server] Signaling READY to GitHub...")
    s2c._refresh_sha()          # pre-fetch SHA so first write succeeds
    ok = s2c.write(b"READY")
    if not ok:
        print("[server] ERROR: failed to write READY — check token/network")
        sys.exit(1)
    print("[server] Waiting for client to connect...")

    while True:
        data = c2s.read_new()
        if data and data.startswith(b"CONNECT"):
            print("[server] Client CONNECT received!")
            break
        time.sleep(POLL_SEC)

    print("[server] Connecting to local SSH daemon...")
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect(("127.0.0.1", LOCAL_SSH))
    sock.settimeout(None)
    print("[server] SSH connected — tunneling now")

    pump(sock, recv_ch=c2s, send_ch=s2c, tag="server")
    print("[server] Session ended. Re-run to accept next connection.")


# ── Client mode ───────────────────────────────────────────────────────────────

def run_client(token, port):
    print("=" * 55)
    print("  GitHub API SSH Tunnel  —  CLIENT MODE")
    print("=" * 55)
    print(f"  Repo        : {REPO}")
    print(f"  Session     : {SESSION}")
    print(f"  Listen port : {port}")
    print()

    c2s = Channel(REPO, token, f"tunnel/{SESSION}/c2s.bin", "write")
    s2c = Channel(REPO, token, f"tunnel/{SESSION}/s2c.bin", "read")

    # Wait for server READY
    print("[client] Waiting for server (Iranian side) to signal READY...")
    deadline = time.time() + 60
    while time.time() < deadline:
        data = s2c.read_new()
        if data and b"READY" in data:
            print("[client] Server is READY!")
            break
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(POLL_SEC)
    else:
        print("\n[client] Timed out waiting for server. Is the server running?")
        sys.exit(1)

    print(f"\n[client] Listening on localhost:{port}")
    print(f"[client] Now run:  ssh -p {port} root@localhost")
    print()

    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(1)

    conn, addr = srv.accept()
    print(f"[client] SSH client connected from {addr}")

    # Tell server to connect to sshd
    c2s.write(b"CONNECT")
    time.sleep(0.3)

    pump(conn, recv_ch=s2c, send_ch=c2s, tag="client")
    print("[client] Session ended.")


# ── Built-in SOCKS5 proxy server ──────────────────────────────────────────────

def _socks5_client(conn):
    """Handle one SOCKS5 client — runs in its own daemon thread."""
    try:
        # Greeting: read version + nmethods
        head = conn.recv(2)
        nmethods = head[1]
        conn.recv(nmethods)            # consume offered auth methods
        conn.send(b'\x05\x00')         # select: no authentication

        # Request
        req  = conn.recv(4)
        cmd  = req[1]
        atyp = req[3]

        if   atyp == 0x01:             # IPv4
            addr = socket.inet_ntoa(conn.recv(4))
        elif atyp == 0x03:             # domain name
            n    = conn.recv(1)[0]
            addr = conn.recv(n).decode()
        elif atyp == 0x04:             # IPv6
            addr = socket.inet_ntop(socket.AF_INET6, conn.recv(16))
        else:
            return

        port = struct.unpack('!H', conn.recv(2))[0]

        if cmd != 0x01:                # only CONNECT is supported
            conn.send(b'\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00')
            return

        # Open upstream TCP connection
        try:
            up  = socket.create_connection((addr, port), timeout=20)
            bnd = up.getsockname()
            conn.send(b'\x05\x00\x00\x01'
                      + socket.inet_aton(bnd[0])
                      + struct.pack('!H', bnd[1]))
        except OSError:
            conn.send(b'\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00')
            return

        # Relay bytes until one side closes
        pair = [conn, up]
        while True:
            r, _, x = select.select(pair, [], pair, 120)
            if x or not r:
                break
            for s in r:
                data = s.recv(8192)
                if not data:
                    return
                (up if s is conn else conn).sendall(data)

    except Exception:
        pass
    finally:
        try: conn.close()
        except: pass


def run_socks5(port):
    """
    Tiny SOCKS5 proxy server — pure stdlib, no auth, CONNECT only.
    Useful as a gateway: run this on the Windows/outside machine, then
    expose it to the Iranian server via SSH reverse tunnel:
        ssh -R 3128:127.0.0.1:<port> -N -tt -p 2222 root@localhost
    Iranian server can then curl --socks5 localhost:3128 https://...
    """
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('127.0.0.1', port))
    srv.listen(128)
    print("=" * 55)
    print("  Built-in SOCKS5 Proxy  —  GATEWAY MODE")
    print("=" * 55)
    print(f"  Listening : 127.0.0.1:{port}")
    print()
    print("  To expose this proxy to the Iranian server, run:")
    print(f"    ssh -R 3128:127.0.0.1:{port} -N -tt -p 2222 root@localhost")
    print()
    print("  On the Iranian server, use the proxy with:")
    print("    export ALL_PROXY=socks5://127.0.0.1:3128")
    print("    curl --socks5 localhost:3128 https://google.com")
    print()
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=_socks5_client, args=(conn,), daemon=True).start()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="SSH tunnel via GitHub API — works through Iran's firewall"
    )
    ap.add_argument("mode",    choices=["server", "client", "socks5"],
                    help="server = run on Iranian VPS  |  client = run on your machine  |  socks5 = run local SOCKS5 gateway")
    ap.add_argument("--token", default=os.environ.get("GITHUB_TOKEN", ""),
                    help="GitHub Personal Access Token (repo scope)")
    ap.add_argument("--port",  type=int, default=CLIENT_PORT,
                    help=f"local port for SSH (client mode, default {CLIENT_PORT})")
    args = ap.parse_args()

    if not args.token and args.mode != "socks5":
        print("ERROR: provide --token or set GITHUB_TOKEN env var")
        print("  Get token at: https://github.com/settings/tokens")
        print("  Scopes needed: repo (read+write)")
        sys.exit(1)

    if args.mode == "server":
        run_server(args.token)
    elif args.mode == "client":
        run_client(args.token, args.port)
    else:  # socks5
        run_socks5(args.port)
