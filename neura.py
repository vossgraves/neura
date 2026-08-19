#!/usr/bin/env python3
"""
neura.py — NeuralWatt free-tier gateway (proxy-rotation engine)
==========================================================
Exposes an OpenAI-compatible API on localhost backed by Neuralwatt's
anonymous playground gateway (https://portal.neuralwatt.com/api/chat).

Free tier facts (per their /api/usage):
  - 5 requests/min, 50 requests/day, 10k tokens/day, 1024 max output tokens
  - Keyed to IP hash, resets daily -> usable indefinitely, throttled
  - No auth, no cookies, no API key required

The gateway forwards `tools` INTO the model's context but suppresses
native tool_call emission. This shim works around that with a prompt
contract: tool schemas are injected into the system prompt, the model
answers with a single JSON object, and the shim re-synthesizes
OpenAI-shaped tool_calls for the agent loop.

Optional egress modes:
  - NW_PROXY=http://user:pass@host:port  fixed single HTTP proxy
  - default (auto): rotating pool of public proxies, each with its own
    daily quota (fetch -> TCP filter -> portal probe -> score by remaining
    budget; failover is in-line). Sources:
      NW_PROXY_LIST_URLS (comma-separated plain host:port or JSON lists)
    Pool knobs: NW_PROXY_MIN_POOL, NW_PROXY_MAX_POOL, NW_PROXY_TIMEOUT,
    NW_PROXY_REFRESH_SEC, NW_DIRECT_FALLBACK. NW_PROXY_MODE=off => direct.
    NOTE: public proxies are unreliable and rotation may violate
    Neuralwatt's ToS — keep the legitimate direct free tier as the
    supported path, and don't hammer the service.

Deps: python3 stdlib only.  Run:  python3 neura.py            (port 8787)
"""

import collections
import json
import os
import random
import re
import secrets
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("NW_SHIM_PORT", "8787"))
TARGET = "https://portal.neuralwatt.com/api/chat"
USAGE_URL = "https://portal.neuralwatt.com/api/usage"
MAX_OUTPUT_TOKENS = 1024  # free-tier server clamp
PROXY = os.environ.get("NW_PROXY", "").strip()
DEBUG = os.environ.get("NW_DEBUG", "") == "1"
UA = os.environ.get("NW_UA", "opencode")  # present as opencode; override with NW_UA

# --- public proxy rotation (see call_portal / _call_with_pool) -------------
# The free tier keys quotas to the egress IP. Rotating public proxies gives
# each pool member its own 50 req / 10k tok daily budget -> effectively
# unlimited. Tune via env:
#   NW_PROXY_MODE        auto (default) | single | off
#   NW_PROXY_LIST_URLS   comma-separated proxy-list URLs (plain host:port
#                        or JSON with ip/port fields)
#   NW_PROXY_MIN_POOL    rebuild pool when it drops below this (default 3)
#   NW_PROXY_MAX_POOL    keep at most this many validated proxies (default 12)
#   NW_PROXY_TIMEOUT     per-proxy probe timeout, seconds (default 6)
#   NW_PROXY_REFRESH_SEC full pool refresh interval (default 600)
#   NW_DIRECT_FALLBACK   1 (default) falls back to own IP when pool is empty
_PROXY_MODE = os.environ.get("NW_PROXY_MODE", "auto").strip().lower()
if PROXY and _PROXY_MODE == "auto":
    _PROXY_MODE = "single"  # legacy: explicit NW_PROXY keeps its behavior
_LIST_URLS = [
    u.strip() for u in os.environ.get(
        "NW_PROXY_LIST_URLS",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt,"
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=yes&anonymity=all,"
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    ).split(",")
    if u.strip()
]
_MIN_POOL = int(os.environ.get("NW_PROXY_MIN_POOL", "3"))
_MAX_POOL = int(os.environ.get("NW_PROXY_MAX_POOL", "20"))
_PROXY_TIMEOUT = float(os.environ.get("NW_PROXY_TIMEOUT", "6"))
_REFRESH_SEC = float(os.environ.get("NW_PROXY_REFRESH_SEC", "600"))
_DIRECT_FALLBACK = os.environ.get("NW_DIRECT_FALLBACK", "1") == "1"
_PER_PROXY_MIN = 4   # headroom under the portal's 5 req/min per-IP ceiling
_MAX_FAILS = 3       # consecutive failures before a proxy is evicted

# ---------------------------------------------------------------------------
# Client-side pacing (direct / single-proxy mode): the free tier allows
# 5 req/min per IP. Windows stall before firing when a burst would exceed
# the ceiling; 429s upstream get a transparent retry.
# ---------------------------------------------------------------------------
_WINDOW = collections.deque()  # timestamps of recent upstream calls
_WINDOW_LOCK = threading.Lock()


def pace_and_record():
    """Sleep until firing now keeps us under 5 req / 60s, then record."""
    with _WINDOW_LOCK:
        now = time.time()
        while _WINDOW and _WINDOW[0] <= now - 60:
            _WINDOW.popleft()
        if len(_WINDOW) >= 5:
            wait = 60 - (now - _WINDOW[0]) + 0.5
            if wait > 0:
                sys.stderr.write(f"[neura] pacing: sleeping {wait:.1f}s for free-tier minute cap\n")
                time.sleep(wait)
                now = time.time()
                while _WINDOW and _WINDOW[0] <= now - 60:
                    _WINDOW.popleft()
        _WINDOW.append(time.time())


# ---------------------------------------------------------------------------
# Rotating public-proxy pool
# ---------------------------------------------------------------------------
class ProxyPool:
    """Thread-safe pool of validated public proxies.

    Each member carries its own daily budget (read from the portal's usage
    endpoint through that proxy) and its own minute-window pacing, so the
    aggregate throughput scales with pool size instead of one IP's caps.
    """

    def __init__(self):
        self._lk = threading.RLock()
        self._pool = []              # list of member dicts
        self._last_refresh = 0.0
        self._last_attempt = 0.0
        self._refreshing = False

    def ready(self):
        with self._lk:
            return bool(self._pool)

    def stats(self):
        with self._lk:
            return {
                "size": len(self._pool),
                "members": [
                    {"addr": p["addr"], "quota": p["quota"], "fails": p["fails"]}
                    for p in self._pool
                ],
            }

    def _find(self, addr):
        for p in self._pool:
            if p["addr"] == addr:
                return p
        return None

    def _mk(self, addr):
        return {
            "addr": addr,
            "quota": 0,                        # requests remaining today
            "tokens": 0,
            "fails": 0,
            "blocked": 0.0,                    # skip until this unix ts
            "used": 0.0,                       # tiebreak: last use
            "window": collections.deque(),     # minute-window request stamps
        }

    def refresh(self, force=False):
        """Fetch candidates, TCP-filter, HTTP-probe the portal through the
        survivors, keep the _MAX_POOL best by remaining quota."""
        with self._lk:
            if self._refreshing:
                return bool(self._pool)
            if not force and time.time() - self._last_refresh < 30:
                return bool(self._pool)
            if self._pool and not force and len(self._pool) >= _MIN_POOL \
                    and time.time() - self._last_refresh < _REFRESH_SEC:
                return True
            self._refreshing = True
        try:
            cands = fetch_candidates()
            if not cands:
                with self._lk:
                    self._last_attempt = time.time()
                return bool(self._pool)
            # stage 1: cheap parallel TCP reachability — bounded + shuffled;
            # public lists are mostly dead weight, and executor futures keep
            # running after we return, so cut off early and let leftovers
            # drain in the background (shutdown(wait=False)).
            probe_set = cands[:500]
            random.shuffle(probe_set)
            live = []
            ex1 = ThreadPoolExecutor(max_workers=24)
            futs1 = {ex1.submit(_tcp_ok, c): c for c in probe_set}
            for f in as_completed(futs1):
                if f.result():
                    live.append(futs1[f])
                if len(live) >= 150:
                    break
            ex1.shutdown(wait=False)
            # stage 2: bounded HTTP probe of the portal usage endpoint
            wanted = max(_MAX_POOL * 2, _MIN_POOL + 2)
            results = []
            random.shuffle(live)
            ex2 = ThreadPoolExecutor(max_workers=8)
            futs2 = {ex2.submit(probe_proxy, c): c for c in live[:100]}
            for f in as_completed(futs2):
                r = f.result()
                if r:
                    results.append(r)
                    if len(results) >= wanted:
                        break
            ex2.shutdown(wait=False)
            results.sort(key=lambda r: (-r[1], r[2]))
            # spread egress diversity: keep the best member per /24 so one
            # hot subnet can't dominate the pool or get us all throttled
            fresh = []
            seen_subnets = set()
            for addr, q, tk in results:
                ip = addr.split(":")[0]
                subnet = ".".join(ip.split(".")[:3]) if ip.count(".") == 3 else ip
                if subnet in seen_subnets:
                    continue
                seen_subnets.add(subnet)
                p = self._find(addr) or self._mk(addr)
                p["quota"] = q
                p["tokens"] = tk
                p["fails"] = 0
                fresh.append(p)
                if len(fresh) >= _MAX_POOL:
                    break
            with self._lk:
                self._pool = fresh
                self._last_refresh = time.time()
            sys.stderr.write(
                f"[neura] proxy pool: {len(fresh)} healthy of {len(results)} successful probes "
                f"(daily quota range {min((r[1] for r in results), default=0)}-"
                f"{max((r[1] for r in results), default=0)} req)\n"
            )
            return bool(fresh)
        except Exception as e:
            sys.stderr.write(f"[neura] proxy refresh failed: {e}\n")
            return bool(self._pool)
        finally:
            with self._lk:
                self._refreshing = False
                self._last_attempt = time.time()

    def pick(self):
        """Best ready member: highest quota, fewest failures, least recently
        used, with minute-window headroom. Returns (addr, wait) — wait > 0
        means every member is minute-capped right now (aggregate ceiling):
        sleep `wait` and pick again."""
        with self._lk:
            now = time.time()
            ready = [p for p in self._pool if p["blocked"] <= now and p["quota"] > 0]
            if not ready:
                ready = [p for p in self._pool if p["blocked"] <= now]
            if not ready:
                return None, 0.0
            ready.sort(key=lambda p: (-p["quota"], p["fails"], p["used"]))
            for p in ready:
                w = p["window"]
                while w and w[0] <= now - 60:
                    w.popleft()
                if len(w) < _PER_PROXY_MIN:
                    p["used"] = now
                    w.append(now)
                    return p["addr"], 0.0
            oldest = min((p["window"][0] for p in ready if p["window"]), default=now)
            wait = min(70.0, max(0.0, 60 - (now - oldest) + 0.2))
            sys.stderr.write(f"[neura] all proxies minute-capped, waiting {wait:.0f}s\n")
            return None, wait

    def ok(self, addr):
        with self._lk:
            p = self._find(addr)
            if p:
                p["fails"] = 0
                p["quota"] = max(0, p["quota"] - 1)  # optimistic; probe re-syncs

    def throttle(self, addr, seconds=60):
        with self._lk:
            p = self._find(addr)
            if p:
                p["blocked"] = time.time() + seconds

    def fail(self, addr):
        with self._lk:
            p = self._find(addr)
            if p:
                p["fails"] += 1
                if p["fails"] >= _MAX_FAILS:
                    self._pool.remove(p)
                    sys.stderr.write(f"[neura] evicting {addr} ({_MAX_FAILS} failures)\n")
                else:
                    p["blocked"] = time.time() + 15

    def evict(self, addr):
        with self._lk:
            p = self._find(addr)
            if p:
                self._pool.remove(p)
                sys.stderr.write(f"[neura] evicting {addr} (daily budget gone)\n")


def _tcp_ok(addr, timeout=3.0):
    host, _, port = addr.rpartition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def _parse_list(text):
    """Parse a proxy list dump: plain host:port lines, or JSON (array of
    {ip, port} or {data: [...]})."""
    out = set()
    t = text.strip()
    if not t:
        return out
    if t[:1] in "[{":
        try:
            data = json.loads(t)
        except Exception:
            return out
        items = data.get("data", data) if isinstance(data, dict) else data
        for it in items or []:
            if isinstance(it, dict):
                ip = it.get("ip") or it.get("address") or it.get("host")
                port = it.get("port")
                if ip and port:
                    out.add(f"{ip}:{port}")
        return out
    for ln in t.splitlines():
        ln = ln.strip()
        if not ln or " " in ln or "/" in ln:
            continue
        host, _, port = ln.partition(":")
        if host and port.isdigit():
            out.add(f"{host}:{port}")
    return out


def fetch_candidates():
    cands = set()
    for url in _LIST_URLS:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            txt = urllib.request.urlopen(req, timeout=12).read().decode("utf-8", "replace")
        except Exception as e:
            sys.stderr.write(f"[neura] proxy list fetch failed ({url}): {e}\n")
            continue
        n = len(cands)
        cands |= _parse_list(txt)
        sys.stderr.write(f"[neura] proxy list {url}: +{len(cands) - n} candidates\n")
    return sorted(cands)


def probe_proxy(addr):
    """(addr, quota, tokens) or None. Validate through the proxy with the
    REAL chat path (tiny POST), then read that IP's remaining quota via the
    usage endpoint. A member that passes this is chat-capable — the usage-GET
    alone proves relay only (some proxies pass GETs but choke on POSTs)."""
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": addr, "https": addr})
    )
    try:
        ping = json.dumps({
            "model": "deepseek-v4-flash",
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 1,
            "stream": True,
        }).encode()
        req = urllib.request.Request(
            TARGET,
            data=ping,
            headers={"Content-Type": "application/json", "User-Agent": UA},
            method="POST",
        )
        raw = opener.open(req, timeout=_PROXY_TIMEOUT).read().decode("utf-8", "replace")
        if not _has_usable_output(raw):
            return None
        ureq = urllib.request.Request(USAGE_URL, headers={"User-Agent": UA})
        u = json.loads(opener.open(ureq, timeout=_PROXY_TIMEOUT).read().decode("utf-8", "replace"))
        return addr, int(u.get("requests_remaining_day", 0)), int(u.get("tokens_remaining_day", 0))
    except (urllib.error.HTTPError, urllib.error.URLError, EOFError, ValueError, OSError):
        return None
    except Exception:
        return None


POOL = ProxyPool()


def _pool_worker():
    """Background maintenance: fills a sparse pool fast, full refresh on TTL."""
    while True:
        try:
            with POOL._lk:
                low = (len(POOL._pool) < _MIN_POOL
                       and time.time() - POOL._last_attempt > 20)
                stale = time.time() - POOL._last_refresh > _REFRESH_SEC
            if low or stale:
                POOL.refresh(force=low)
        except Exception:
            pass
        time.sleep(15)


def opener_for(addr):
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": addr, "https": addr})
    )


def proxy_budget_left(addr):
    """True when the portal (seen through this proxy) still has daily quota.
    Only called on throttle signals — polling costs request slots."""
    try:
        req = urllib.request.Request(USAGE_URL, headers={"User-Agent": UA})
        data = json.loads(opener_for(addr).open(req, timeout=10).read().decode())
        return data.get("requests_remaining_day", 0) > 0 and data.get("tokens_remaining_day", 0) > 0
    except Exception:
        return True


def call_portal(payload, retries=3):
    """POST to the portal. Returns (status, body_bytes).

    Direct / single-proxy modes: local sliding-window pacing (5 req / 60s)
    keeps us under the per-IP minute cap without touching the usage endpoint;
    the usage endpoint is only consulted AFTER a throttle signal (429 or
    empty stream) to tell daily-exhaustion apart from minute-busy.

    Auto (rotating proxy) mode: each request rides a pooled public proxy,
    each with its own daily budget. Failover is in-line — a dead or
    throttled member is skipped and the next candidate takes the request.
    """
    if _PROXY_MODE == "auto":
        return _call_with_pool(payload)

    pace_and_record()
    req = urllib.request.Request(
        TARGET,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "User-Agent": UA,
        },
        method="POST",
    )
    for attempt in range(retries + 1):
        try:
            resp = OPENER.open(req, timeout=120)
            return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries:
                if not _daily_budget_left():
                    return 429, b'{"error": {"message": "free-tier daily budget exhausted (50 req / 10k tok per IP-day); resets at midnight UTC"}}'
                sys.stderr.write(f"[neura] 429 from upstream, waiting 60s before retry {attempt + 1}/{retries}\n")
                time.sleep(60)
                continue
            return e.code, e.read()
        except Exception as e:
            sys.stderr.write(f"[neura] upstream exception: {type(e).__name__}: {e}\n")
            return 502, str(e).encode()


def _proxy_attempt(addr, payload, timeout=20):
    """One proxy POST. Returns (kind, addr, data):
      ('ok', addr, raw_bytes)         usable 200 stream
      ('throttle', addr, None)        429, or 200 with no usable output
      ('http', addr, (code, raw))     other HTTP status
      ('error', addr, message)        transport failure
    """
    req = urllib.request.Request(
        TARGET,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "User-Agent": UA,
        },
        method="POST",
    )
    try:
        resp = opener_for(addr).open(req, timeout=timeout)
        raw = resp.read()
        if resp.status == 200:
            if _has_usable_output(raw.decode("utf-8", "replace")):
                return ("ok", addr, raw)
            return ("throttle", addr, None)  # silent-throttle stream
        return ("http", addr, (resp.status, raw))
    except urllib.error.HTTPError as e:
        if e.code == 429:
            return ("throttle", addr, None)
        return ("http", addr, (e.code, None))
    except Exception as e:
        return ("error", addr, str(e))


def _call_with_pool(payload, deadline_sec=180, parallel=3):
    """Rotating-proxy POST with PARALLEL failover: each round fires the
    request through up to `parallel` distinct proxies at once and takes the
    fastest success. Sequential cycling wastes the deadline on dying relays;
    racing three from a fresh pool spreads the odds. Losers are demoted /
    evicted by failure kind; the round repeats until the deadline."""
    deadline = time.time() + deadline_sec
    last = (502, b'{"error": {"message": "no proxy attempt made"}}')
    if not POOL.ready():
        sys.stderr.write("[neura] pool empty; warming up (first call may take ~30s)\n")
        POOL.refresh(force=True)
    while time.time() < deadline:
        picks = []
        for _ in range(parallel):
            addr, wait = POOL.pick()
            if addr:
                picks.append(addr)
            elif wait > 0:
                time.sleep(min(wait, max(0.0, deadline - time.time())))
            else:
                break
        if not picks:
            if not POOL.ready():
                # Distinguish "pool has never been populated / still warming"
                # from "pool was healthy and just died" — only fall back to
                # direct egress in the latter case, and only after a refresh
                # actually concluded.
                with POOL._lk:
                    ever_populated = POOL._last_refresh > 0
                if not ever_populated:
                    POOL.refresh(force=False)
                    time.sleep(5)
                    continue
                POOL.refresh(force=True)
                if _DIRECT_FALLBACK:
                    sys.stderr.write("[neura] proxy pool empty — falling back to direct egress (own IP quota)\n")
                    return _call_direct(payload)
                return 503, b'{"error": {"message": "no usable public proxy in pool; retry later or set NW_PROXY_MODE=off"}}'
            continue
        ex = ThreadPoolExecutor(max_workers=len(picks))
        futs = {ex.submit(_proxy_attempt, a, payload): a for a in picks}
        try:
            for f in as_completed(futs):
                kind, addr, data = f.result()
                sys.stderr.write(f"[neura] proxy attempt {addr}: {kind}\n")
                if kind == "ok":
                    POOL.ok(addr)
                    sys.stderr.write(f"[neura] chat served via {addr}\n")
                    return 200, data
                elif kind == "throttle":
                    if proxy_budget_left(addr):
                        POOL.throttle(addr, 60)
                        last = (429, b'{"error": {"message": "throttled through proxy; rotated egress"}}')
                    else:
                        POOL.evict(addr)
                elif kind == "http":
                    code, raw = data
                    if code == 429:
                        POOL.throttle(addr, 60)
                        last = (429, b'{"error": {"message": "429 through proxy; rotated egress"}}')
                    else:
                        POOL.fail(addr)
                        last = (code, raw or b'{"error": {"message": "non-200 through proxy"}}')
                else:
                    POOL.fail(addr)
                    last = (502, str(data).encode())
        finally:
            ex.shutdown(wait=False)
    return last


def _call_direct(payload):
    """Single direct-egress attempt (used when the pool is empty and
    NW_DIRECT_FALLBACK=1). No retry loop — the pool path handles retries."""
    pace_and_record()
    req = urllib.request.Request(
        TARGET,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "User-Agent": UA,
        },
        method="POST",
    )
    try:
        resp = OPENER.open(req, timeout=120)
        return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:
        return 502, str(e).encode()


def _daily_budget_left():
    """True when the portal still has daily request/token quota. Only called
    on throttle signals, never in the happy path (polls cost slots)."""
    try:
        req = urllib.request.Request(USAGE_URL, headers={"User-Agent": UA})
        data = json.loads(OPENER.open(req, timeout=10).read().decode())
        return data.get("requests_remaining_day", 0) > 0 and data.get("tokens_remaining_day", 0) > 0
    except Exception:
        return True

# ---------------------------------------------------------------------------
# Model catalog (free endpoint accepts the full /v1/models list)
# ---------------------------------------------------------------------------
MODELS = {
    "deepseek-v4-flash":           {"name": "DeepSeek V4 Flash",          "context": 1048000, "thinking_default": "off"},
    "deepseek-ai/DeepSeek-V4-Flash": {"name": "DeepSeek V4 Flash (raw)",   "context": 1048000, "thinking_default": "off"},
    "deepseek-v4-flash-flex":      {"name": "DeepSeek V4 Flash Flex",     "context": 1048000, "thinking_default": "off"},
    "glm-5.2":                     {"name": "GLM-5.2",                    "context": 1048000, "thinking_default": "max"},
    "glm-5.2-fast":                {"name": "GLM-5.2 Fast",               "context": 1048000, "thinking_default": "off"},
    "glm-5.2-flex":                {"name": "GLM-5.2 Flex",               "context": 1048000, "thinking_default": "max"},
    "glm-5.2-short":               {"name": "GLM-5.2 Short",              "context": 199000,  "thinking_default": "max"},
    "glm-5.2-short-fast":          {"name": "GLM-5.2 Short Fast",         "context": 199000,  "thinking_default": "off"},
    "glm-5.2-short-flex":          {"name": "GLM-5.2 Short Flex",         "context": 199000,  "thinking_default": "max"},
    "glm-5.2-short-fast-flex":     {"name": "GLM-5.2 Short Fast Flex",    "context": 199000,  "thinking_default": "off"},
    "gemma-4-31b":                 {"name": "Gemma 4 31B",                "context": 262000,  "thinking_default": "off", "vision": True},
    "kimi-k2.7-code":              {"name": "Kimi K2.7 Code",             "context": 262000,  "thinking_default": "on"},
    "kimi-k2.7-code-fast":         {"name": "Kimi K2.7 Code Fast",        "context": 262000,  "thinking_default": "light"},
    "kimi-k2.7-code-flex":         {"name": "Kimi K2.7 Code Flex",        "context": 262000,  "thinking_default": "on"},
    "kimi-k3":                     {"name": "Kimi K3",                    "context": 1048000, "thinking_default": "max"},
    "kimi-k3-fast":                {"name": "Kimi K3 Fast",               "context": 1048000, "thinking_default": "off"},
    "kimi-k3-flex":                {"name": "Kimi K3 Flex",               "context": 1048000, "thinking_default": "max"},
    "qwen3.6-35b":                 {"name": "Qwen3.6 35B",                "context": 131000,  "thinking_default": "on"},
    "qwen3.6-35b-fast":            {"name": "Qwen3.6 35B Fast",           "context": 131000,  "thinking_default": "off"},
}

# ---------------------------------------------------------------------------
# Tool contract
# ---------------------------------------------------------------------------
def _estimate_tokens(text):
    """Rough token estimate without a tokenizer: ~4 chars/token for prose.
    Over-estimates slightly (1.3x) so we stay under the portal's hard 1024
    prompt-token cap with margin."""
    return max(1, int(len(text) // 4 * 1.3) + 1)


def _compact_tool(fn):
    """Render a tool schema in the minimum tokens the model needs to call it:
    name, a cut-down description, and parameter names with types. The portal
    counts prompt tokens against the trial cap, and omp ships 16 chunky MCP
    schemas — full descriptions would blow the budget before the conversation
    even starts."""
    params = fn.get("parameters") or {}
    props = params.get("properties") or {}
    plist = []
    for pname, pspec in props.items():
        ptype = pspec.get("type", "any") if isinstance(pspec, dict) else "any"
        plist.append(f"{pname}:{ptype}")
    req = params.get("required") or []
    required = " required:" + ",".join(req) if req else ""
    desc = (fn.get("description") or "").strip().replace("\n", " ")
    if len(desc) > 120:
        desc = desc[:117].rstrip() + "..."
    line = f"{fn.get('name', 'tool')}({','.join(plist)}){required}"
    if desc:
        line += f" - {desc}"
    return line


def build_contract(tools, tool_choice):
    """Build the system-prompt appendix that lets the gateway model do
    tool calling without native tool_call emission. Tools are COMPACTED:
    the portal only sees the text — raw schemas never leave the shim."""
    lines = [
        "[TOOL PROTOCOL]",
        "You are running inside an agent harness that DOES NOT support native function calling.",
        "When you need to use a tool, reply with ONLY one JSON object, no markdown fences, no prose:",
        '  {"tool_calls": [{"name": "<tool name>", "arguments": {<args per schema>}}]}',
        "You may include several calls in the array at once.",
        "When you do NOT need a tool, reply with ONLY:",
        '  {"text": "<your reply>"}',
        "Never output anything other than one of those two JSON objects.",
        "",
        "AVAILABLE TOOLS:",
    ]
    for i, tool in enumerate(tools, 1):
        fn = tool.get("function", tool)
        lines.append(f"{i}. {_compact_tool(fn)}")

    if isinstance(tool_choice, dict) and tool_choice.get("function", {}).get("name"):
        lines.append("")
        lines.append(f"You MUST call the tool named '{tool_choice['function']['name']}'.")
    elif tool_choice == "required":
        lines.append("")
        lines.append("You MUST call one of the tools.")
    return "\n".join(lines)


def inject_contract(messages, tools, tool_choice):
    """Return a copy of messages with the contract appended to the system
    prompt, and historical tool_calls / tool results flattened to text
    (the gateway's chat template is strict about message roles)."""
    out = []
    contract = build_contract(tools, tool_choice) if tools else None
    system_seen = False
    for m in messages:
        role = m.get("role", "user")
        content = m.get("content", "")
        if isinstance(content, list):  # multimodal content arrays
            parts = []
            for p in content:
                if isinstance(p, dict):
                    if p.get("type") == "text":
                        parts.append(p.get("text", ""))
                elif isinstance(p, str):
                    parts.append(p)
            content = " ".join(parts)
        if role == "system":
            system_seen = True
            if contract:
                content = (content + "\n\n" + contract) if content else contract
            out.append({"role": "system", "content": content})
        elif role == "assistant":
            tc = m.get("tool_calls")
            if tc:
                calls_txt = []
                for c in tc:
                    fn = c.get("function", {})
                    calls_txt.append(
                        f"I called tool {fn.get('name', '?')} with arguments "
                        f"{fn.get('arguments', '{}')} (call id {c.get('id', '?')})."
                    )
                if content:
                    out.append({"role": "assistant", "content": str(content) + " " + " ".join(calls_txt)})
                else:
                    out.append({"role": "assistant", "content": " ".join(calls_txt)})
            else:
                out.append({"role": "assistant", "content": str(content)})
        elif role == "tool":
            out.append({
                "role": "user",
                "content": f"Tool result for call {m.get('tool_call_id', '?')}: {content}",
            })
        else:
            out.append({"role": "user", "content": str(content)})
    if not system_seen and contract:
        out.insert(0, {"role": "system", "content": contract})
    return out


# ---------------------------------------------------------------------------
# Prompt-budget fitting: the portal hard-caps the TOTAL prompt at 1024
# tokens per request for the anonymous tier. Compact contracts help, but
# agent clients (omp ships 16 MCP tools + a 10KB system prompt) still blow
# past it — trim old context until it fits. The last user turn and the
# system/contract prefix always survive.
# ---------------------------------------------------------------------------
MAX_PROMPT_TOKENS = 1020  # portal cap 1024, minus safety margin


def fit_prompt_budget(messages):
    def est(m):
        return _estimate_tokens(m.get("content", "")) if isinstance(m.get("content", ""), str) else 0

    if sum(est(m) for m in messages) <= MAX_PROMPT_TOKENS:
        return messages
    out = [dict(m) for m in messages]
    # 1. merge system messages and trim the middle until the merged system
    #    fits in half the budget (the rest belongs to user context + tools)
    sys_texts = [m["content"] for m in out
                 if m.get("role") == "system" and isinstance(m.get("content"), str)]
    non_sys = [m for m in out if m.get("role") != "system"]
    if sys_texts:
        merged = "\n\n".join(t.strip() for t in sys_texts if t.strip())
        while est({"content": merged}) > MAX_PROMPT_TOKENS // 2 and len(merged) > 400:
            mid = len(merged) // 2
            head, tail = merged[:mid], merged[mid:]
            merged = (head[: len(head) * 3 // 4] + "\n...[trimmed]...\n" + tail[len(tail) // 4 :])
        out = [{"role": "system", "content": merged}] + non_sys
    # 2. drop oldest turns from the front (never the final user message)
    while len(out) > 1 and sum(est(m) for m in out) > MAX_PROMPT_TOKENS:
        idx = 1 if out[0].get("role") == "system" else 0
        del out[idx]
    # 3. last resort: truncate the tail of the final user message
    if sum(est(m) for m in out) > MAX_PROMPT_TOKENS:
        last = out[-1]
        room = MAX_PROMPT_TOKENS - sum(est(m) for m in out[:-1])
        chars = max(200, int(room * 3))
        if isinstance(last.get("content"), str):
            last["content"] = last["content"][-chars:]
        else:
            last["content"] = str(last["content"])[-chars:]
    return out


# ---------------------------------------------------------------------------
# Tool-call JSON parsing (handles fences, leading prose, brace matching)
# ---------------------------------------------------------------------------
def looks_like_json(text):
    t = text.lstrip()
    t = re.sub(r"^```(?:json)?\s*", "", t)
    return t.startswith("{") or t.startswith("[")


def strip_fences(text):
    return re.sub(r"^\s*```(?:json)?\s*", "", text).rstrip().rstrip("`").strip()


def parse_tool_json(text):
    """Return (kind, payload) where kind is 'tool_calls', 'text', or None."""
    t = strip_fences(text)
    start = t.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    esc = False
    end = -1
    for i in range(start, len(t)):
        ch = t[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break
    if end == -1:
        return None
    try:
        obj = json.loads(t[start : end + 1])
    except Exception:
        return None
    if isinstance(obj, dict):
        if obj.get("tool_calls"):
            return ("tool_calls", obj["tool_calls"])
        if "text" in obj and isinstance(obj["text"], str):
            return ("text", obj["text"])
        if "content" in obj and isinstance(obj["content"], str):
            return ("text", obj["content"])
    return None


def synthesize_tool_calls(calls):
    """Map contract-style calls to OpenAI tool_calls objects with ids."""
    out = []
    for c in calls:
        if isinstance(c, str):
            name, args = c, {}
        else:
            name = c.get("name", "unknown_tool")
            args = c.get("arguments", {})
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except Exception:
                    args = {"raw": args}
        out.append({
            "id": "call_" + secrets.token_hex(4),
            "type": "function",
            "function": {"name": name, "arguments": json.dumps(args, separators=(",", ":"))},
        })
    return out


# ---------------------------------------------------------------------------
# Portal upstream call
# ---------------------------------------------------------------------------
def upstream_opener():
    if PROXY:
        handler = urllib.request.ProxyHandler({
            "http": PROXY,
            "https": PROXY,
        })
        return urllib.request.build_opener(handler)
    return urllib.request.build_opener()


OPENER = upstream_opener()


def _has_usable_output(raw):
    """True when the upstream stream actually contains content or tool_calls."""
    try:
        for ln in raw.splitlines():
            s = ln.strip()
            if not s.startswith("data:"):
                continue
            d = s[5:].strip()
            if d == "[DONE]":
                continue
            p = json.loads(d)
            delta = (p.get("choices") or [{}])[0].get("delta") or {}
            if delta.get("content") or delta.get("tool_calls"):
                return True
    except Exception:
        pass
    return False


def forward_params(body):
    """Whitelist what we forward upstream; clamp max_tokens to the free cap.
    omp sends max_completion_tokens (OpenAI responses style) — map it onto
    max_tokens, which the portal understands."""
    p = {}
    for key in ("model", "temperature", "top_p", "stop", "reasoning_effort",
                "response_format", "chat_template_kwargs"):
        if key in body:
            p[key] = body[key]
    if "stream_options" in body and isinstance(body["stream_options"], dict):
        p["stream_options"] = {"include_usage": True}  # portal-compatible subset
    mt = body.get("max_tokens") or body.get("max_completion_tokens")
    p["max_tokens"] = min(int(mt), MAX_OUTPUT_TOKENS) if mt else MAX_OUTPUT_TOKENS
    p["stream"] = True  # portal only streams SSE reliably; we re-serve both modes
    return p


def portal_lines(raw):
    """Split SSE text into complete lines, dropping comment lines."""
    for line in raw.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith(":"):
            continue  # : pricing / : energy / : routing comments
        if s.startswith("data:"):
            yield "data:" + s[5:]


def final_usage_chunk(portal_json):
    usage = portal_json.get("usage") if isinstance(portal_json, dict) else None
    if not usage:
        return None
    return {
        "id": portal_json.get("id", "chatcmpl-shim"),
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": portal_json.get("model", ""),
        "choices": [],
        "usage": usage,
    }


# ---------------------------------------------------------------------------
# OpenAI-compatible handlers
# ---------------------------------------------------------------------------
def handle_chat(body):
    stream = bool(body.get("stream"))
    model = body.get("model", "deepseek-v4-flash")
    messages = body.get("messages", [])
    tools = body.get("tools")
    tool_choice = body.get("tool_choice", "auto")

    if DEBUG:
        sys.stderr.write(
            f"[neura] chat req: model={model} stream={stream} tools={bool(tools)} "
            f"tool_choice={tool_choice} msgs={len(messages)} "
            f"keys={sorted(body.keys())}\n"
        )
        with open("/tmp/neura-last-request.json", "w") as f:
            json.dump(body, f, indent=2)
        if tools:
            sys.stderr.write(
                f"[neura] tool names: {[t.get('function', t).get('name') for t in tools]}\n"
            )

    tool_mode = bool(tools) and tool_choice != "none"
    if tool_mode:
        messages = inject_contract(messages, tools, tool_choice)

    # Fit the 1024-token trial prompt cap BEFORE forwarding; drop raw tools
    # from the payload entirely — the portal only ever sees the compacted
    # contract text, never the schemas (it would double-count them anyway).
    payload = forward_params(body)
    payload.pop("tools", None)
    payload["model"] = model
    payload["messages"] = fit_prompt_budget(messages)

    raw = None
    for _ in range(3):
        try:
            status, body = call_portal(payload)
            if status != 200:
                try:
                    err = json.loads(body.decode("utf-8", errors="replace"))
                except Exception:
                    err = {"error": {"message": body.decode("utf-8", errors="replace")[:300]}}
                return status, err
            raw = body.decode("utf-8", errors="replace")
            # A usable completion must contain at least one content or
            # tool_calls delta. Usage-only streams are the portal's silent
            # throttle variant — wait for the window and retry.
            if DEBUG:
                sys.stderr.write(f"[neura] upstream raw len={len(raw)} usable={_has_usable_output(raw)}\n")
                sys.stderr.write(f"[neura] upstream raw head: {raw[:300]!r}\n")
            if _has_usable_output(raw):
                break
            if not _daily_budget_left():
                return 429, {"error": {"message": "free-tier daily budget exhausted (50 req / 10k tok per IP-day); resets at midnight UTC"}}
            sys.stderr.write(f"[neura] throttled empty stream from upstream, waiting 60s before retry {attempt + 1}/3\n")
            time.sleep(60)
        except Exception as e:
            return 502, {"error": {"message": f"upstream failure: {e}"}}

    if not stream:
        return serve_nonstream(raw, model, tool_mode)

    # stream: serve lines as they arrive where possible
    def gen():
        yield from serve_stream(raw, model, tool_mode)

    return 200, gen()


# ----------------------------- non-stream ----------------------------------
def serve_nonstream(raw, model, tool_mode):
    lines = list(portal_lines(raw))
    data_parts = []
    for ln in lines:
        if ln.startswith("data:"):
            d = ln[5:].strip()
            if d == "[DONE]":
                continue
            try:
                data_parts.append(json.loads(d))
            except Exception:
                pass
    if not data_parts:
        return 502, {"error": {"message": "upstream returned no data frames"}}

    usage = None
    content = ""
    finish = "stop"
    for p in data_parts:
        if p.get("usage"):
            usage = p["usage"]
        fr = (p.get("choices") or [{}])[0].get("finish_reason")
        if fr:
            finish = fr
        delta = (p.get("choices") or [{}])[0].get("delta") or {}
        if delta.get("content"):
            content += delta["content"]

    message = {"role": "assistant", "content": None}
    if content:
        if tool_mode and looks_like_json(content):
            parsed = parse_tool_json(content)
            if parsed:
                kind, val = parsed
                if kind == "tool_calls":
                    message["tool_calls"] = synthesize_tool_calls(val)
                    finish = "tool_calls"
                elif kind == "text":
                    message["content"] = val
                else:
                    message["content"] = content
            else:
                message["content"] = content
        else:
            message["content"] = content

    resp = {
        "id": "chatcmpl-shim-" + secrets.token_hex(6),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
        "usage": usage or {},
    }
    return 200, resp


# ------------------------------ streaming ----------------------------------
def _chunk(model, delta, finish=None, usage=None, cid=None):
    c = {"index": 0, "delta": delta, "finish_reason": finish}
    if usage is not None:
        return {
            "id": cid or "chatcmpl-shim-" + secrets.token_hex(6),
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [],
            "usage": usage,
        }
    return {
        "id": cid or "chatcmpl-shim-" + secrets.token_hex(6),
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [c],
    }


def serve_stream(raw, model, tool_mode):
    cid = "chatcmpl-" + secrets.token_hex(6)
    usage = None
    # tool-mode sniffing state
    sniff = ""
    sniff_state = "pending"  # pending -> json | text
    buffered_json = ""

    def resolve_tool_mode():
        """Emit buffered tool-mode output as tool_calls or text chunks."""
        if DEBUG:
            sys.stderr.write(f"[neura] resolve: state={sniff_state} jsonlen={len(buffered_json)} sniff={len(sniff)}\n")
        if sniff_state == "json":
            parsed = parse_tool_json(buffered_json)
            if parsed:
                kind, val = parsed
                if kind == "tool_calls":
                    for i, tc in enumerate(synthesize_tool_calls(val)):
                        yield json.dumps(_chunk(model, {"tool_calls": [{"index": i, **tc}]}, cid=cid), separators=(",", ":")) + "\n"
                    yield json.dumps(_chunk(model, {}, "tool_calls", cid=cid), separators=(",", ":")) + "\n"
                    return
                elif kind == "text":
                    yield json.dumps(_chunk(model, {"content": val}, cid=cid), separators=(",", ":")) + "\n"
                    return
            # invalid JSON: flush what we held as plain text
            if buffered_json:
                yield json.dumps(_chunk(model, {"content": buffered_json}, cid=cid), separators=(",", ":")) + "\n"
        elif sniff_state == "pending" and sniff:
            yield json.dumps(_chunk(model, {"content": sniff}, cid=cid), separators=(",", ":")) + "\n"

    for ln in portal_lines(raw):
        if ln.startswith("data:"):
            d = ln[5:].strip()
            if d == "[DONE]":
                if tool_mode:
                    yield from resolve_tool_mode()
                if usage:
                    yield json.dumps(_chunk(model, {}, usage=usage, cid=cid), separators=(",", ":")) + "\n"
                yield "data: [DONE]\n"
                return
            try:
                p = json.loads(d)
            except Exception:
                continue
            if p.get("usage"):
                usage = p["usage"]
            if p.get("error"):
                yield json.dumps({"error": p["error"]}, separators=(",", ":")) + "\n"
                yield "data: [DONE]\n"
                return
            delta = (p.get("choices") or [{}])[0].get("delta") or {}
            fr = (p.get("choices") or [{}])[0].get("finish_reason")
            # reasoning passthrough (aliased for opencode's interleaved field)
            if delta.get("reasoning"):
                yield json.dumps(_chunk(model, {"reasoning": delta["reasoning"], "reasoning_content": delta["reasoning"]}, cid=cid), separators=(",", ":")) + "\n"
            content_piece = delta.get("content")
            if not content_piece:
                # portal's final empty-delta chunk carries finish_reason
                if fr and not tool_mode:
                    yield json.dumps(_chunk(model, {}, fr, cid=cid), separators=(",", ":")) + "\n"
                continue

            if not tool_mode:
                yield json.dumps(_chunk(model, {"content": content_piece}, cid=cid), separators=(",", ":")) + "\n"
                continue

            # ---- tool mode: sniff then decide ----
            if sniff_state == "pending":
                sniff += content_piece
                probe = sniff.lstrip()
                if DEBUG:
                    sys.stderr.write(f"[neura] sniff pending len={len(probe)} head={probe[:60]!r}\n")
                if len(probe) >= 40 or "```" in probe:
                    if probe.startswith("{") or probe.startswith("```"):
                        sniff_state = "json"
                        buffered_json = probe
                    else:
                        sniff_state = "text"
                        yield json.dumps(_chunk(model, {"content": sniff}, cid=cid), separators=(",", ":")) + "\n"
                        sniff = ""
            elif sniff_state == "json":
                buffered_json += content_piece
            else:  # text
                yield json.dumps(_chunk(model, {"content": content_piece}, cid=cid), separators=(",", ":")) + "\n"

    # ---- stream end (no [DONE] seen): resolve leftovers ----
    if tool_mode:
        yield from resolve_tool_mode()
    if usage:
        yield json.dumps(_chunk(model, {}, usage=usage, cid=cid), separators=(",", ":")) + "\n"
    yield "data: [DONE]\n"


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("[neura] %s\n" % (fmt % args))

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return b""
        return self.rfile.read(length)

    def _send(self, code, obj, ctype="application/json"):
        data = json.dumps(obj).encode() if not isinstance(obj, (bytes, str)) else (
            obj.encode() if isinstance(obj, str) else obj
        )
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_stream(self, gen):
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            # HTTP/1.1: chunked framing so clients that need a real stream
            # end (omp, httpx, opencode) can parse the SSE cleanly instead
            # of hanging on a raw-close response.
            for line in gen:
                chunk = line.encode("utf-8")
                self.wfile.write(f"{len(chunk):X}\r\n".encode() + chunk + b"\r\n")
                self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        if self.path.rstrip("/") in ("/v1/models", "/models"):
            data = {
                "object": "list",
                "data": [
                    {
                        "id": mid,
                        "object": "model",
                        "created": 0,
                        "owned_by": "neuralwatt",
                        "max_model_len": info["context"],
                        "metadata": {
                            "display_name": info["name"],
                            "capabilities": {
                                "tools": True,
                                "vision": bool(info.get("vision")),
                                "reasoning": info["thinking_default"] != "off",
                            },
                        },
                    }
                    for mid, info in MODELS.items()
                ],
            }
            self._send(200, data)
            return
        if self.path.rstrip("/") in ("/v1/usage", "/usage"):
            try:
                req = urllib.request.Request(USAGE_URL, headers={"User-Agent": UA})
                raw = OPENER.open(req, timeout=20).read().decode()
                data = json.loads(raw)
                if _PROXY_MODE == "auto":
                    data["egress_pool"] = POOL.stats()
                self._send(200, data)
            except Exception as e:
                self._send(502, {"error": {"message": str(e)}})
            return
        self._send(404, {"error": {"message": "not found"}})

    def do_POST(self):
        if self.path.rstrip("/") in ("/v1/chat/completions", "/chat/completions"):
            raw = self._read_body()
            try:
                body = json.loads(raw or b"{}")
            except Exception:
                self._send(400, {"error": {"message": "invalid JSON body"}})
                return
            code, result = handle_chat(body)
            if isinstance(result, dict):
                self._send(code, result)
                return
            if hasattr(result, "__iter__"):
                self._send_stream(result)
                return
            self._send(code, result)
            return
        self._send(404, {"error": {"message": "not found"}})


def main():
    if _PROXY_MODE == "auto":
        threading.Thread(target=_pool_worker, daemon=True).start()
        print("[neura] proxy rotation enabled — each egress IP carries its own 50 req / 10k tok daily quota", flush=True)
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    egress = "direct" if _PROXY_MODE == "off" else (
        "single proxy" if _PROXY_MODE == "single" else "rotating proxy pool")
    print(f"[neura] Neuralwatt free bridge on http://127.0.0.1:{PORT}/v1", flush=True)
    print(f"[neura] Upstream: {TARGET}  (egress: {egress})", flush=True)
    print(f"[neura] Free tier: 50 req/day, 10k tok/day, {MAX_OUTPUT_TOKENS} max output tokens", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()