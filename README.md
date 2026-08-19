# neura

**NeuralWatt, free and effectively unlimited — a single-file, stdlib-only
gateway that turns the anonymous free tier into a rotating pool of daily
quotas.**

Neura is a local OpenAI-compatible API server (port `8787`) that talks to
NeuralWatt's anonymous free endpoint — no account, no key. Because the free
tier keys its daily budget (50 requests / 10k tokens) to the **egress IP**,
neura rotates egress through a self-maintaining pool of public proxies so the
quota multiplies by the pool size. 6 healthy proxies, for example, is a
~300 req / 60k token daily budget that reboots itself while you sleep.

The same server also works for the **paid API** — or anything else that
speaks OpenAI-compatible HTTP.

---

## How the "unlimited" works

| Tier | Quota key | Default ceiling | With neura auto mode |
|---|---|---|---|
| anonymous free | egress IP | 50 req / 10k tok per day | × pool size (rotates) |
| paid | account (API key) | your balance | unaffected, direct |

The pool lifecycle (`auto` mode, default):

1. **Fetch** — pulls candidate proxies from public HTTP proxy lists
   (monosans, TheSpeedX, proxyscrape, etc).
2. **Filter** — TCP-connects candidates in parallel, keeps the live ones.
3. **Probe** — for each survivor, HTTP-probes the portal *through* the proxy;
   a parsed 200 on `/api/usage` proves relay + target reachability and
   returns that IP's remaining daily budget.
4. **Score & keep** — keeps the best `NW_PROXY_MAX_POOL` members by remaining
   quota.
5. **Rotate** — one request per proxy, paced under the portal's 5 req/min
   per-IP ceiling, in-line failover on dead members, eviction after 3
   failures or at 0 quota, background refill every `NW_PROXY_REFRESH_SEC`.

Your own IP is left untouched — every free request spends someone else's
quota, never yours.

## Features

- **Single file, Python 3 stdlib only.** No pip, no node, no containers.
- **OpenAI-compatible**: `/v1/chat/completions` (with streaming + tool
  calls), `/v1/models`, `/v1/usage`.
- **Tool-call aware**: compacts tool schemas into short per-tool contracts
  and trims context to fit the free tier's 1,024-token prompt cap — agent
  loops (omp, opencode, anything) keep running under the limit.
- **Self-pacing**: 5 req/min clamp, 429 backoff, transparent retries, clear
  errors when a daily budget is genuinely gone.
- **UA spoof**: presents itself as `opencode` upstream by default
  (`NW_UA` to override) — the full model catalog answers to any client.
- **Modes**: `auto` (pool rotation), `single` (one proxy), `off` (direct).
- **Full catalog**: all 18+ NeuralWatt models served: deepseek-v4-flash
  (+flex), glm-5.2 (fast/flex/short/short-fast/short-flex/short-fast-flex),
  kimi-k2.7-code (fast/flex), kimi-k3 (fast/flex), qwen3.6-35b (fast),
  gemma-4-31b (vision).

## Quick start

```bash
git clone https://github.com/vossgraves/neura.git
cd neura
python3 neura.py &            # starts on 127.0.0.1:8787
```

Then point anything OpenAI-compatible at `http://127.0.0.1:8787/v1`:

```bash
curl http://127.0.0.1:8787/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"hi"}],"max_tokens":100}'
```

Watch the pool come up in the console — first chat may take ~10-20 s while
proxies probe; after that the pool holds.

For the paid tier, either keep using the shim (`model` ids are identical) or
point your client at `https://api.neuralwatt.com/v1` with your key.

## Configuration

All via environment variables. Copy `.env.example` and adjust:

| Variable | Default | What it does |
|---|---|---|
| `NW_SHIM_PORT` | `8787` | listen port |
| `NW_PROXY_MODE` | `auto` | `auto` \| `single` \| `off` |
| `NW_PROXY` | *(empty)* | explicit proxy (`http://ip:port`) when mode is `single` |
| `NW_PROXY_MIN_POOL` | `3` | minimum healthy proxies to maintain |
| `NW_PROXY_MAX_POOL` | `20` | maximum pool members kept at once |
| `NW_PROXY_TIMEOUT` | `6` | seconds per proxy attempt on real requests |
| `NW_PROBE_TIMEOUT` | `5` | seconds per proxy probe during cold-start pool sweep |
| `NW_PROXY_REFRESH_SEC` | `600` | background refill interval |
| `NW_DIRECT_FALLBACK` | `1` | fall back to direct egress if the pool empties |
| `NW_UA` | `opencode` | User-Agent presented to the portal |
| `NW_DEBUG` | *(unset)* | `1` prints verbose request/response traces |

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /v1/models` | model catalog (id, context length, capabilities) |
| `POST /v1/chat/completions` | chat + streaming + tools; `max_tokens` clamped to 1,024 on the free tier |
| `GET /v1/usage` | live free-tier status: requests/tokens today, remaining, current pool size and per-proxy quotas |
| `GET /v1/quota` | (paid mode) account balance — requires `NEURALWATT_API_KEY` |

## Integrations

### oh-my-pi (`omp`)

`examples/omp-models.yml` registers **both** providers in
`~/.omp/agent/models.yml` — `neuralwatt-free` (shim, 1K output cap) and
`neuralwatt` (paid, 8.2K output) — with all 18 models each. Roles default to
the free provider:

```bash
omp models find neuralwatt          # validate + browse
omp config set modelRoles.default neuralwatt-free/deepseek-v4-flash
```

### OpenCode

`examples/opencode-provider.json` merges both providers into
`~/.config/opencode/opencode.json` under `provider`, then pick with
`/models`. The free provider is keyless; the paid provider reads
`NEURALWATT_API_KEY`.

### Quota on the command line

`./nw-usage` prints the free tier status from the local shim, or the paid
account balance when `NEURALWATT_API_KEY` is set.

## Honest caveats

- Public proxies are mostly datacenter boxes and throwaways: expect a chunk
  of every list to be dead, slow, or TLS-broken. The pool self-heals, but
  throughput tracks list quality — first minutes after a cold start are the
  slowest.
- Some egresses are blocked by NeuralWatt's edge (403) — those are filtered
  out during probing, not retried.
- The free tier sits on top of NeuralWatt's anonymous endpoint and its
  limits. Rotation multiplies quotas against that endpoint's design; if
  you're building something that matters, pay for the API — the shim speaks
  to both.
- The 1,024-token prompt cap is real: long agent contexts get trimmed
  (system messages merged, middles dropped, newest turns kept). Keep prompts
  tight on the free tier; switch roles to the paid provider for heavy work.

## License

MIT — see `LICENSE`.