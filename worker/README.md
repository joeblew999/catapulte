# catapulte on Cloudflare Workers (slice 1)

Proof-of-life that catapulte's hexagonal core runs on Workers via `workers-rs`.
The same `domain` crate that the native binary uses is wired to a D1-backed
storage adapter and served from a `fetch` handler.

## What works now

- `GET /health/live` → `ok`
- `POST /emails` → persists an envelope to D1 (inline `plain` / `mjml_inline` /
  `mjml_named` bodies; recipients; params)
- `GET /emails` → lists stored emails as JSON

This exercises the three things that had to be proven on Workers:
1. the `domain` crate compiles to `wasm32` (it needed a tokio-feature fix —
   see `domain/Cargo.toml`),
2. the D1 binding works through the `EmailRepository` port
   (`adapter/outbound-d1`),
3. the `!Send` D1 handle/futures bridge to the `Send + Sync` domain ports via
   `SendWrapper`/`SendFuture`.

## What's deferred to later slices

- The real `inbound-http` axum `router()` (reusable — it's already separable
  from the socket bind) instead of the hand-wired routes here.
- **Sending** via the `send_email` binding (today the worker only persists).
- MRML rendering — `outbound-mjml` needs a wasm build (drop tokio + the
  reqwest/fs template loaders, fetch remote templates via `worker::Fetch`).
- CF Queues consumer (`#[event(queue)]`) + cron GC (`#[event(scheduled)]`).
- R2 attachment store; `lifecycle_events` for real delivery status.

## Deploy

```sh
wrangler d1 create catapulte                      # paste database_id into wrangler.toml
wrangler d1 migrations apply catapulte
wrangler deploy                                    # runs worker-build
```

Local build only (no account needed):

```sh
worker-build --release          # produces build/worker/shim.mjs
```

## Why this lives outside the cargo workspace

`worker/` and `adapter/outbound-d1/` are `wasm32`-only and are listed under
`exclude` in the root `Cargo.toml`, so the native `cargo build --workspace`
stays green. They build via `worker-build` (which compiles for `wasm32`).
