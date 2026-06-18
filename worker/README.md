# catapulte on Cloudflare Workers

catapulte's hexagonal core running on Workers via `workers-rs`, with **one
Durable Object** providing storage + queue + scheduling.

## Architecture

```
fetch ──▶ CatapulteStore (Durable Object)
            ├─ embedded SQLite  (emails + email_queue)   ← replaces D1
            ├─ alarm()          (drain queue, backoff)   ← replaces CF Queues + cron
            └─ serialized exec  (no row-claim races)
```

One primitive instead of D1 + Queues + cron. It maps 1:1 onto catapulte's
native "DB-as-queue + background poller" model — the DO `alarm()` *is* the
poller, and the DO's single-threaded execution removes the `claimed_until`
race the native sqlite backend handles by hand.

Crates:
- `adapter/outbound-sql-core` — wire DTOs shared by sqlite / D1 / DO.
- `adapter/outbound-do` — `DoStore`: `EmailRepository` + queue ops over DO
  SQLite. No `SendWrapper` needed (`SqlStorage` is `Send+Sync`, `exec` is sync).
- `adapter/outbound-d1` — D1 alternative (kept for multi-writer setups).
- `worker/` — the `#[durable_object]` + `fetch`/`alarm` handlers.

## What works now

- `GET /health/live` → `ok`
- `POST /emails` → persists + enqueues + arms the alarm
- `GET /emails` → lists stored emails
- `alarm()` → claims due queue rows, retries with capped exponential backoff,
  re-arms for the next due entry

## What's deferred (next slice)

**Real delivery.** `deliver()` in `worker/src/lib.rs` is the injection point:
render the MJML body and send via the `send_email` binding. Today it is a
no-op success so the queue/alarm machinery runs end to end. Also deferred:
reusing the real `inbound-http` axum `router()`, MRML wasm build, R2
attachments, and real delivery-event status.

## Deploy

```sh
mise run worker:deploy        # wrangler deploy — provisions the DO + SQLite migration
mise run worker:tail          # logs
```

Local build only (no account needed):

```sh
mise run worker:build         # worker-build --release → build/worker/shim.mjs
```

The DO's SQLite schema is created in code (`DoStore::init_schema`, run on every
activation) — there is no D1 database to create and no SQL migration files.

## Why these crates live outside the cargo workspace

`worker/`, `adapter/outbound-do/` and `adapter/outbound-d1/` are `wasm32`-only
and listed under `exclude` in the root `Cargo.toml`, so native
`cargo build --workspace` stays green. They build via `worker-build`.
`outbound-sql-core` *is* a normal member (pure serde, compiles both ways).
