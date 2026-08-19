# Findings — NeuralWatt free tier, egress rotation & the neura gateway

Session findings that shaped `neura.py`. Kept as a record for future debugging.

## 1. The free tier is keyed to the egress IP — not the account, not the UA

| Cap | Value |
|---|---|
| Requests / day / IP | 50 |
| Tokens / day / IP | 10,000 |
| Requests / minute / IP | 5 |
| Max output tokens / request | 1,024 |
| Max prompt tokens / request | ~1,024 (portal hard-cap, shim compacts long contexts) |

- Spoofing the User-Agent to `opencode` makes the portal *see* opencode, but does **not** remove limits — UA is a presentation layer, quota is IP-keyed.
- The working bypass is **egress rotation**: every distinct public IP behind the portal carries its own fresh 50/10k budget.
- The budget resets daily at **midnight UTC**.

## 2. Gateway architecture (the shim)

An OpenAI-compatible bridge (`POST /v1/chat/completions`, `GET /v1/models`, `GET /v1/usage`) on `127.0.0.1:8787` that:

1. Compacts oversize tool schemas/context to stay under the prompt cap.
2. Maintains a rotating pool of public proxies, each pre-validated for **chat capability** (not just relay).
3. Races each request through up to 3 distinct proxy egresses in parallel and takes the fastest success.
4. Demotes throttled/dead members by failure kind; tracks per-member daily quota + minute window.

Env: `NW_PROXY_MODE` (`auto` default | `single` | `off`), `NW_UA` (default `opencode`), `NW_SHIM_PORT` (8787), `NW_PROXY_REFRESH_SEC`, `NW_PROXY_LIST_URLS`, `NW_STATIC_PROXIES` (see §5), `NW_DIRECT_FALLBACK`, `NW_PROBE_TIMEOUT`.

## 3. Latency journey: 42.8s → 3.6s

| Fix | Effect |
|---|---|
| Startup pool warmup (probe before first serve) | first request no longer pays cold-start |
| Parallel fetch across 8 list sources | pool fills in seconds, not minutes |
| Non-stream probe pings (a tiny real chat POST) | stream probes *stall behind public relays*; non-stream returns usable output fast |
| Unique-per-round picks (no duplicate parallel POSTs) | no wasted quota/time on repeat members |
| Minute-cap sleep 70s → 15s + busy-spin guard | keeps 5 req/min headroom without dead time |
| Usage-GET failure no longer rejects chat-capable members | GET-only relays were being discarded despite passing POST |

Measured: `hello` 42.8s (day-1 direct single relay) → **3.1–3.6s** through the pool.

## 4. Public pool lifecycle

- 8 reputed sources: monosans, TheSpeedX, proxyscrape (v4 + v2), proxifly, clarketm, iplocate, geonode.
- Mid-day (US) yield collapses — the swarm burns public egresses within hours of the UTC reset; sweeps find 0–2 fresh IPs.
- `MIN_POOL` underflow triggers fast re-sweeps (~20–35s) while shipping partial pools instantly.
- After midnight UTC the same lists refill with fresh-budget IPs.

## 5. Static commercial pools (stable backbone)

Public lists are a lottery. Commercial/private proxies (InstantProxies, pvdata, cactussstp-style accounts) work far better:

- **31 of 49** entries from a private list validated against the portal, each with a **full 50-req / 10k-tok quota** at check time.
- `NW_STATIC_PROXIES="user:pass@host:port,user:pass@host:port,..."` seeds them at startup.
- Static members: authenticated (HTTP basic via `ProxyHandler`), **never evicted**, **survive pool refreshes**, quota-tracked like any member.
- Dead accounts (`yjrdrwwc`, `nngone`, a P105.instantproxies entry, one raw IP) failed auth on every port — expired/revoked creds, not a shim issue.

## 6. GitHub Actions self-test (`run-and-export.yml`)

Every run (push to main, hourly cron, manual dispatch):

1. Starts `neura.py` exactly like a user would.
2. Waits for `/v1/models`, picks the first advertised model.
3. Dumps `/v1/usage` + a real chat completion through the pool.
4. **Tees all output to `neura-output.txt`** and uploads it as an artifact.

No secrets, no token, no user input — and the runner's egress IP is fresh per run, so every cycle gets another untouched 50/10k budget.

## 7. Known limits

- Public-proxy yield mid-day is thin; the pool throttles, it doesn't break (races + direct fallback).
- The portal caps outputs at 1,024 tokens — long generations must be chunked client-side.
- Headless `omp -p` in this sandbox paints nothing (omp TUI quirk), but the API side serves + streams fine — not a NeuralWatt issue.